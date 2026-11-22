# clog —— 带 RabbitMQ 消费能力的 C++ 日志库

一个轻量的 C++17 日志库，包含两条日志通路：

1. **本地直写**：业务进程调用 `LOG_INFO(...)`，异步写入本地文件；
2. **消息队列汇聚**：日志先投递到 RabbitMQ，由日志服务器上的
   `RabbitMQConsumer` 从队列取出后统一落盘。

两条通路最终都汇入同一个 `AsyncLogger`，因此日志文件格式完全一致。

---

## 1. 目录结构

| 文件 | 作用 |
| --- | --- |
| `LogLevel.h` | 日志级别枚举，级别与字符串互转 |
| `log_message.h` | 日志记录结构 `LogRecord`、格式化 / 解析 / MQ 消息规范化 |
| `logfile.h` | 日志文件管理：按天 + 序号滚动、按大小切分、线程安全的时间函数 |
| `async_logger.h` | 异步日志器：入队 + 后台线程批量落盘 + 背压 |
| `printlog.h` | 对外接口：`LOG_DEBUG/INFO/WARN/ERROR/FATAL` 宏 |
| `rabbitmq_config.h` | RabbitMQ 连接 / 拓扑 / 消费 / 落盘配置 |
| `rabbitmq_detail.h` | rabbitmq-c 的薄封装（连接、声明拓扑、错误文本） |
| `rabbitmq_consumer.h` | **RabbitMQ 消费者**：从队列取日志并落盘（核心新增） |
| `rabbitmq_publisher.h` | RabbitMQ 发布者：把日志投递到 MQ（联调 / 业务使用） |
| `mq_consumer_main.cpp` | 日志服务器示例 |
| `mq_publisher_main.cpp` | 发布测试日志示例 |
| `main.cpp` | 本地异步日志示例 |
| `Makefile` | 构建脚本 |

---

## 2. 架构

```
业务进程（本地直写）
  LOG_INFO(...) ──> PrintLog ──> AsyncLogger::append ──┐
                                                       │
业务进程（投递到 MQ）                                   │
  PublishLog(...) ──> RabbitMQPublisher ──publish──>    │
                                                       │
                    RabbitMQ Exchange（topic）          │
                              │ route "log.#"           │
                              ▼                         │
                        RabbitMQ Queue                  │
                              │ basic.consume           │
                              ▼                         │
  日志服务器：RabbitMQConsumer::Run()                   │
    ├─ NormalizeMqBody() 规范化消息                     │
    └─ AsyncLogger::append ─────────────────────────────┘
                              │
                              ▼
                    AsyncLogger（后台线程）
                              │ 批量 write + flush
                              ▼
                    logfile（按天/按大小滚动）
                              │
                              ▼
                 log/log_20261111_0.txt ...
```

关键点：**消费者不直接写文件**，而是把消息交给 `AsyncLogger`，
复用同一套批量落盘、滚动切分逻辑，避免重复实现。

---

## 3. 构建

依赖（Ubuntu / WSL）：

```bash
sudo apt-get install -y g++ make librabbitmq-dev libssl-dev
```

构建：

```bash
make            # 构建 main / mq_consumer / mq_publisher
make main       # 仅构建本地日志示例（不依赖 RabbitMQ）
make mq         # 仅构建 RabbitMQ 消费者与发布者
make clean      # 清理产物
```

> 链接 RabbitMQ 相关程序时需要 `-lrabbitmq -lssl -lcrypto -pthread`，
> Makefile 与 `.vscode/tasks.json` 已经配置好。

---

## 4. 快速开始

### 4.1 本地日志（不经过 MQ）

```bash
./main
tail -n 5 log/log_$(date +%Y%m%d)_0.txt
```

输出示例：

```
[2026-11-11 23:14:37] [INFO] main.cpp:15 | 线程 9 第 1 条日志，端口 8080
[2026-11-11 23:14:37] [ERROR] main.cpp:26 | 多线程测试结束，线程数=10，总日志=50
```

### 4.2 RabbitMQ 全链路

终端 A（日志服务器）：

```bash
MQ_LOG_DIR=log_mq/ ./mq_consumer
```

终端 B（发布日志）：

```bash
./mq_publisher 10 50     # 发送 10 条，间隔 50ms
```

消费者退出（`Ctrl+C`）后查看：

```bash
tail -n 20 log_mq/*.txt
```

输出示例：

```
[2026-11-11 23:14:49] [INFO] rabbitmq-consumer | 已连接 RabbitMQ host=127.0.0.1:5672 vhost=/ queue=log.queue
[2026-11-11 23:14:52] [INFO] mq_publisher:main | 第 1 条结构化日志
[2026-11-11 23:14:52] [INFO] mq:log.app1 | 裸消息 #5（未格式化）
[2026-11-11 23:14:52] [INFO] mq_publisher:main | 第 2 条结构化日志
...
[2026-11-11 23:14:55] [INFO] rabbitmq-consumer | 消费者已退出
```

---

## 5. 日志与消息格式

标准日志行（本地输出与 MQ 传输共用）：

```
[2026-11-11 23:08:34] [INFO] main.cpp:42 | hello world
 └─ 时间戳 ─────────┘ └级别┘ └来源─────┘ └正文─────┘
```

消费者对不同消息的处理策略：

| 消息体 | 处理方式 |
| --- | --- |
| 首行已是标准格式 | **原样保留**（尊重生产端的时间戳 / 级别 / 来源） |
| 多行标准日志（含堆栈） | 整段原样保留 |
| 裸文本 / 多行裸文本 | 用「当前时间 + INFO + `mq:<routing_key>`」包装 |
| 空消息 | 跳过落盘（但仍会 ack，避免反复投递） |

---

## 6. RabbitMQ 配置

`RabbitMQConfig`（`rabbitmq_config.h`）字段：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `host` / `port` | `127.0.0.1` / `5672` | broker 地址 |
| `vhost` | `/` | 虚拟主机 |
| `user` / `password` | `guest` / `guest` | 账号（guest 默认仅 localhost 可登录） |
| `heartbeat` | `0` | 心跳秒数，0 表示关闭 |
| `exchange` / `exchange_type` | `log.exchange` / `topic` | 交换机 |
| `queue` | `log.queue` | 队列 |
| `routing_key` | `log.#` | 绑定键，可匹配 `log.app1` 等 |
| `consumer_tag` | `clog-consumer` | 消费者标签 |
| `auto_ack` | `false` | `false` 为手动 ack，处理完才确认 |
| `prefetch` | `100` | QoS 预取条数 |
| `consume_timeout_ms` | `1000` | 单次拉取超时（决定 Stop 响应速度） |
| `reconnect_delay_ms` | `3000` | 断线重连间隔 |
| `log_dir` / `log_basename` | `log/` / `log` | 落盘目录与文件名前缀 |

`mq_consumer` 支持用环境变量覆盖常用项：

```bash
MQ_HOST=127.0.0.1 MQ_PORT=5672 MQ_VHOST=/ \
MQ_USER=guest MQ_PASSWORD=guest \
MQ_EXCHANGE=log.exchange MQ_QUEUE=log.queue MQ_ROUTING_KEY='log.#' \
MQ_LOG_DIR=log_mq/ MQ_LOG_BASENAME=log \
./mq_consumer
```

---

## 7. 设计要点

### 7.1 异步批量落盘

- `append()` 只入队，业务线程不做磁盘 IO；
- 后台线程在「攒够 200 条」或「等待超过 200ms」时被唤醒；
- 一次把整批日志拼成一个字符串，只调用一次 `write + flush`。

### 7.2 背压策略

队列上限 10 万条。超过后**丢弃最旧的一条**，并累加 `dropped_` 计数。
这是「有损但保命」的策略：宁可丢日志，也不让内存无限增长拖垮进程。

### 7.3 文件滚动

文件名形如 `log_20261111_0.txt`：

- 跨天：切换到新一天的文件，序号归零；
- 单文件超过 100MB：序号 +1，换新文件；
- 尽量在换行处切分，避免把一条日志劈成两半；
- 单行超过上限时硬写，避免死循环。

### 7.4 消费可靠性

- **手动 ack**：消息交给本地日志器之后才 `basic.ack`，broker 才会删除；
- **QoS 预取**：限制未确认消息数量，防止消息一次性灌满客户端内存；
- **断线重连**：网络抖动 / broker 主动关闭后，最多 3 秒重连；
- **异常连接直接销毁**：收到 `connection.close` / 协议错误时，
  不调用 `channel_close`（可能阻塞），而是直接 `amqp_destroy_connection`
  后重连；
- **RPC 超时**：所有管理类 RPC 设置 3 秒超时，避免半关闭连接导致永久阻塞；
- **优雅停止**：`Stop()` 置标志即可，消费循环最多一个 `consume_timeout_ms`
  周期内退出，并排空日志队列。

### 7.5 与本地日志共用落盘

消费者把规范化后的日志交给 `AsyncLogger::append()`，
因此 MQ 日志与本地日志共用同一个 `logfile` 单例、同一套滚动策略。
消费者自身的运行日志（连接、重连、退出）也写入同一个文件，
来源标记为 `rabbitmq-consumer`，便于排查。

---

## 8. 联调与验证建议

1. 启动本地 RabbitMQ（示例用 Docker）：

   ```bash
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
   ```

   管理台：<http://localhost:15672>（guest / guest）。

2. 终端 A 启动消费者，终端 B 发布日志，观察 `log_mq/` 下的文件。

3. 验证断线重连：

   ```bash
   docker exec <container> rabbitmqctl close_all_user_connections guest "test"
   ```

   消费者应在日志中打印「连接中断」，随后自动重连并继续消费。

4. 验证消息不丢：消费者停止期间发布消息，队列（durable）会积压；
   重新启动消费者后，积压消息会被消费并落盘。

---

## 9. 已知边界

- `AsyncLogger` / `logfile` 都是进程级单例，参数只在**第一次**调用时生效；
  日志服务器应在 `main` 开头用最终配置初始化。
- 未实现日志文件保留（按份数/天数清理）策略，建议用 `logrotate` 等外部工具。
- 发布者未开启 publisher confirm；对日志场景通常可接受，
  若要求「绝不丢」可改为 mandatory + confirm 模式。
- 本库面向 Linux/POSIX 环境；Windows 下部分系统调用（如 `mkdir`）已做兼容分支，
  但未做完整验证。

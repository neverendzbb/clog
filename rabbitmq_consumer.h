#ifndef CLOG_RABBITMQ_CONSUMER_H
#define CLOG_RABBITMQ_CONSUMER_H

/**
 * @file rabbitmq_consumer.h
 * @brief RabbitMQ 日志消费者：从消息队列取日志，落盘到本地日志文件。
 *
 * 数据流：
 *
 *   业务进程 ──publish──> RabbitMQ Exchange ──route──> Queue
 *                                                         │
 *                                    RabbitMQConsumer::Run() │ basic.consume
 *                                                         ▼
 *                                       NormalizeMqBody()  规范化
 *                                                         ▼
 *                                       AsyncLogger::append() 入队
 *                                                         ▼
 *                                       logfile 后台线程批量落盘
 *
 * 关键特性：
 *   - 手动 ack：只有成功交给本地日志器之后才确认，避免消息丢失；
 *   - QoS 预取：限制未确认消息数量，防止把 broker 的消息全灌进内存；
 *   - 断线重连：网络抖动/ broker 重启后自动恢复，无需人工干预；
 *   - 优雅停止：Stop() 可在任意线程调用，循环会在一个轮询周期内退出；
 *   - 统计信息：received/written/ack_failed/reconnects，便于监控。
 */

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <sys/time.h>  // struct timeval
#endif

#include "async_logger.h"
#include "log_message.h"
#include "rabbitmq_config.h"
#include "rabbitmq_detail.h"

class RabbitMQConsumer {
public:
    /**
     * @brief 构造。此时尚未建立网络连接。
     */
    explicit RabbitMQConsumer(RabbitMQConfig cfg) : cfg_(std::move(cfg)) {}

    ~RabbitMQConsumer() { Close(); }

    // 禁止拷贝：内部持有网络句柄和原子计数。
    RabbitMQConsumer(const RabbitMQConsumer&) = delete;
    RabbitMQConsumer& operator=(const RabbitMQConsumer&) = delete;

    /**
     * @brief 主动建立一次连接（可选，用于“启动即失败”的快速反馈）。
     *
     * Run() 内部也会自动连接，因此一般不必手动调用。
     * @return true 表示连接 + 拓扑声明 + 消费者注册全部成功。
     */
    bool Connect() {
        const std::string verr = cfg_.Validate();
        if (!verr.empty()) {
            LogInternal(LogLevel::ERROR, "配置非法: " + verr);
            return false;
        }
        if (connected_.load(std::memory_order_relaxed)) return true;

        std::string err;
        if (!Open(err)) {
            LogInternal(LogLevel::ERROR, "连接失败: " + err);
            return false;
        }
        connected_.store(true, std::memory_order_relaxed);
        LogInternal(LogLevel::INFO, "已连接 RabbitMQ host=" + cfg_.host + ":" +
                                        std::to_string(cfg_.port) +
                                        " vhost=" + cfg_.vhost +
                                        " queue=" + cfg_.queue);
        return true;
    }

    /**
     * @brief 阻塞运行消费循环，直到 Stop() 被调用或配置非法。
     *
     * 循环内部自动处理断线重连；正常情况下不会返回。
     * 建议放在独立线程中运行。
     */
    void Run() {
        const std::string verr = cfg_.Validate();
        if (!verr.empty()) {
            LogInternal(LogLevel::ERROR, "配置非法，消费者无法启动: " + verr);
            return;
        }

        running_.store(true, std::memory_order_relaxed);

        while (running_.load(std::memory_order_relaxed)) {
            // —— 未连接：先建连 ——
            if (!connected_.load(std::memory_order_relaxed)) {
                std::string err;
                if (!Open(err)) {
                    LogInternal(LogLevel::ERROR,
                                "连接失败: " + err + "，将在 " +
                                    std::to_string(cfg_.reconnect_delay_ms) +
                                    "ms 后重试");
                    if (!WaitReconnect()) break;
                    continue;
                }
                connected_.store(true, std::memory_order_relaxed);
                LogInternal(LogLevel::INFO,
                            "已连接 RabbitMQ host=" + cfg_.host + ":" +
                                std::to_string(cfg_.port) +
                                " vhost=" + cfg_.vhost +
                                " queue=" + cfg_.queue);
            }

            // —— 已连接：拉取并处理一条消息 ——
            // 返回 false 表示连接已不可用，需要走重连流程。
            if (!ConsumeOnce()) {
                connected_.store(false, std::memory_order_relaxed);
                Close();
                reconnects_.fetch_add(1, std::memory_order_relaxed);
                LogInternal(LogLevel::WARN, "与 broker 的连接中断，准备重连");
                if (!WaitReconnect()) break;
            }
        }

        Close();
        connected_.store(false, std::memory_order_relaxed);
        LogInternal(LogLevel::INFO, "消费者已退出");
    }

    /**
     * @brief 请求停止消费循环（线程安全，可从信号处理/其它线程调用）。
     *
     * 注意：这里只置标志并唤醒，不阻塞等待；等待退出由调用方 join 完成。
     */
    void Stop() { running_.store(false, std::memory_order_relaxed); }

    // ---- 运行状态与统计（无锁读取，适合监控线程周期采集） ----
    bool connected() const {
        return connected_.load(std::memory_order_relaxed);
    }
    std::uint64_t received() const {
        return received_.load(std::memory_order_relaxed);
    }
    std::uint64_t written() const {
        return written_.load(std::memory_order_relaxed);
    }
    std::uint64_t ack_failed() const {
        return ack_failed_.load(std::memory_order_relaxed);
    }
    std::uint64_t reconnects() const {
        return reconnects_.load(std::memory_order_relaxed);
    }

private:
    /**
     * @brief 建连 + 声明拓扑 + 注册消费者。
     *
     * 任一步失败都会关闭已建立的连接，保证 conn_ 为 nullptr。
     */
    bool Open(std::string& err) {
        if (conn_ != nullptr) return true;

        if (!mq_detail::OpenConnection(cfg_, conn_, channel_, err)) {
            return false;
        }

        // 依次声明 exchange、queue、binding，并设置 QoS。
        // 用短路求值：任一步失败立即进入统一清理。
        // 失败后直接 Abort（销毁句柄），避免对可能已异常的连接发 close RPC。
        if (!mq_detail::DeclareExchange(conn_, channel_, cfg_, err) ||
            !mq_detail::DeclareQueue(conn_, channel_, cfg_, err) ||
            !mq_detail::BindQueue(conn_, channel_, cfg_, err) ||
            !mq_detail::SetQos(conn_, channel_,
                               static_cast<std::uint16_t>(cfg_.prefetch), err)) {
            Abort();
            return false;
        }

        // 注册消费者；no_ack 由配置决定。
        amqp_basic_consume(conn_, channel_, amqp_cstring_bytes(cfg_.queue.c_str()),
                           amqp_cstring_bytes(cfg_.consumer_tag.c_str()),
                           /*no_local=*/0,
                           /*no_ack=*/cfg_.auto_ack ? 1 : 0,
                           /*exclusive=*/0, amqp_empty_table);

        amqp_rpc_reply_t r = amqp_get_rpc_reply(conn_);
        if (r.reply_type != AMQP_RESPONSE_NORMAL) {
            err = "basic_consume 失败: " + mq_detail::ReplyText(r);
            Abort();
            return false;
        }
        return true;
    }

    // 关闭连接（幂等，仅用于健康连接）。
    void Close() {
        if (conn_ == nullptr) return;
        mq_detail::CloseConnection(conn_, channel_);
    }

    // 连接异常时直接销毁句柄，不发送 close RPC（避免阻塞在等待回包）。
    void Abort() { mq_detail::AbortConnection(conn_); }

    /**
     * @brief 拉取并处理一条消息。
     * @return false 表示连接已不可用，调用方应重连。
     */
    bool ConsumeOnce() {
        // 释放上一轮已处理消息占用的协议缓冲区，避免内存缓慢增长。
        amqp_maybe_release_buffers(conn_);

        // 单次最多等待 consume_timeout_ms，保证 Stop() 能及时生效。
        struct timeval tv;
        tv.tv_sec = cfg_.consume_timeout_ms / 1000;
        tv.tv_usec = (cfg_.consume_timeout_ms % 1000) * 1000;

        amqp_envelope_t envelope;
        amqp_rpc_reply_t reply = amqp_consume_message(conn_, &envelope, &tv, 0);

        // 正常收到一条 basic.deliver。
        if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
            HandleMessage(envelope);
            amqp_destroy_envelope(&envelope);
            return true;
        }

        // 超时：队列暂时没有消息，属正常情况。
        if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
            reply.library_error == AMQP_STATUS_TIMEOUT) {
            return true;
        }

        // 收到非投递帧（channel.close / connection.close 等）。
        if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
            reply.library_error == AMQP_STATUS_UNEXPECTED_STATE) {
            if (!HandleUnexpectedFrame()) {
                // 连接已不可用：直接销毁句柄，不调用 close RPC，
                // 否则可能阻塞在等待一个永远不会到来的回包。
                Abort();
                return false;
            }
            return true;
        }

        // 其它错误：网络断开、协议错误等，直接销毁并重连。
        Abort();
        return false;
    }

    /**
     * @brief 处理 amqp_consume_message 返回的“非投递帧”。
     *
     * 该函数被调用时，引起异常的帧已经被 put back，
     * 必须先调用 amqp_simple_wait_frame() 把它读出来，否则会卡住。
     *
     * @return false 表示收到 close 帧，需要重连。
     */
    bool HandleUnexpectedFrame() {
        amqp_frame_t frame;
        if (amqp_simple_wait_frame(conn_, &frame) != AMQP_STATUS_OK) {
            return false;
        }

        // 心跳帧等非方法帧由库内部处理，这里直接忽略。
        if (frame.frame_type != AMQP_FRAME_METHOD) return true;

        switch (frame.payload.method.id) {
            case AMQP_BASIC_ACK_METHOD:
                // 发布确认，消费者一般不会收到；忽略。
                return true;

            case AMQP_BASIC_RETURN_METHOD: {
                // 不可路由的 mandatory 消息被退回，需要把消息体读掉。
                amqp_message_t msg;
                amqp_rpc_reply_t r =
                    amqp_read_message(conn_, frame.channel, &msg, 0);
                if (r.reply_type == AMQP_RESPONSE_NORMAL) {
                    amqp_destroy_message(&msg);
                }
                return true;
            }

            case AMQP_CHANNEL_CLOSE_METHOD:
                // 通道级错误（如队列被删除），重连即可。
                return false;

            case AMQP_CONNECTION_CLOSE_METHOD: {
                // 连接级错误。先礼貌回复 close-ok，避免 broker 空等到超时；
                // 随后由调用方 Abort() 销毁句柄并重连。
                amqp_connection_close_ok_t ok;
                amqp_send_method(conn_, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &ok);
                return false;
            }

            default:
                // 其它方法帧（如 basic.cancel），忽略并继续。
                return true;
        }
    }

    /**
     * @brief 处理一条已接收的消息：规范化 -> 入队 -> ack。
     */
    void HandleMessage(const amqp_envelope_t& env) {
        received_.fetch_add(1, std::memory_order_relaxed);

        // 消息体可能为空（例如心跳类消息），规范化后为空则跳过落盘。
        const std::string body = mq_detail::BytesToStr(env.message.body);
        const std::string routing = mq_detail::BytesToStr(env.routing_key);
        const std::string tag =
            routing.empty() ? ("mq:" + cfg_.queue) : ("mq:" + routing);

        const std::string line = NormalizeMqBody(body, tag);
        if (!line.empty()) {
            // 与本地日志共用同一个 AsyncLogger，文件格式完全一致。
            AsyncLogger::Instance(cfg_.log_dir, cfg_.log_basename).append(line);
            written_.fetch_add(1, std::memory_order_relaxed);
        }

        // 手动 ack：确认“已经交给本地日志器”，broker 才会删除消息。
        if (!cfg_.auto_ack) {
            const int rc = amqp_basic_ack(conn_, channel_, env.delivery_tag, 0);
            if (rc != AMQP_STATUS_OK) {
                ack_failed_.fetch_add(1, std::memory_order_relaxed);
                // ack 失败通常意味着连接已断，下一轮 ConsumeOnce 会检测到。
            }
        }
    }

    /**
     * @brief 可中断的重连等待。
     * @return false 表示等待期间收到了 Stop()，调用方应退出循环。
     */
    bool WaitReconnect() {
        constexpr int kStepMs = 50;  // 小步长检查停止标志，保证退出及时
        int waited = 0;
        while (running_.load(std::memory_order_relaxed) &&
               waited < cfg_.reconnect_delay_ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kStepMs));
            waited += kStepMs;
        }
        return running_.load(std::memory_order_relaxed);
    }

    /**
     * @brief 消费者自身的运行日志（也写入同一个日志文件）。
     *
     * 单独抽出是为了避免使用 LOG_* 宏时把 AsyncLogger 用默认参数初始化，
     * 从而绕过 cfg_.log_dir 配置。
     */
    void LogInternal(LogLevel lv, const std::string& msg) {
        LogRecord rec;
        rec.timestamp = NowTimestamp();
        rec.level = lv;
        rec.source = "rabbitmq-consumer";
        rec.message = msg;
        AsyncLogger::Instance(cfg_.log_dir, cfg_.log_basename)
            .append(FormatLogRecord(rec));
    }

private:
    RabbitMQConfig         cfg_;
    amqp_connection_state_t conn_ = nullptr;
    amqp_channel_t          channel_ = 1;

    std::atomic<bool>          running_{false};
    std::atomic<bool>          connected_{false};
    std::atomic<std::uint64_t> received_{0};    // 收到的消息数
    std::atomic<std::uint64_t> written_{0};     // 成功落盘的消息数
    std::atomic<std::uint64_t> ack_failed_{0};  // ack 失败次数
    std::atomic<std::uint64_t> reconnects_{0};  // 重连次数
};

#endif  // CLOG_RABBITMQ_CONSUMER_H

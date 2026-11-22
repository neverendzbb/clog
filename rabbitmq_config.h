#ifndef CLOG_RABBITMQ_CONFIG_H
#define CLOG_RABBITMQ_CONFIG_H

/**
 * @file rabbitmq_config.h
 * @brief RabbitMQ 连接与拓扑配置。
 *
 * 一个 RabbitMQConsumer / RabbitMQPublisher 只依赖这一个配置结构，
 * 便于从配置文件、环境变量或命令行统一构建。
 */

#include <cstdint>
#include <string>

struct RabbitMQConfig {
    // ------------------------------------------------------------------
    // 连接参数
    // ------------------------------------------------------------------
    std::string host = "223.4.145.218";   // broker 地址
    int         port = 5672;          // AMQP 端口（管理端口通常是 15672）
    std::string vhost = "/";          // 虚拟主机；不同业务可用不同 vhost 隔离
    std::string user = "admin";       // 用户名；guest 默认只能从 localhost 登录
    std::string password = "123456";   // 密码

    // 心跳间隔（秒）。0 表示关闭。
    // 开启后会周期性收发心跳，便于及时发现“假死”的连接；
    // 但需要消费循环足够频繁地处理网络事件，否则可能被 broker 判定超时。
    int heartbeat = 0;

    // 连接协商参数。0 表示使用库默认值：
    //   channel_max -> AMQP_DEFAULT_MAX_CHANNELS(2047)
    //   frame_max   -> AMQP_DEFAULT_FRAME_SIZE(131072)
    int channel_max = 0;
    int frame_max = 0;

    // ------------------------------------------------------------------
    // 拓扑参数（exchange / queue / binding）
    // ------------------------------------------------------------------
    std::string exchange = "log.exchange";        // 交换机名
    std::string exchange_type = "topic";          // direct / fanout / topic / headers
    bool        exchange_durable = true;          // 持久化交换机（重启后仍存在）
    bool        exchange_auto_delete = false;     // 无绑定后是否自动删除

    std::string queue = "log.queue";              // 队列名
    bool        queue_durable = true;             // 持久化队列（重启后仍存在）
    bool        queue_exclusive = false;          // 排他队列（仅当前连接可用）
    bool        queue_auto_delete = false;        // 最后一个消费者断开后是否删除

    std::string routing_key = "log.#";            // 绑定键；"log.#" 可匹配 log.xxx

    // ------------------------------------------------------------------
    // 消费参数
    // ------------------------------------------------------------------
    std::string consumer_tag = "clog-consumer";   // 消费者标签（便于在管理台识别）
    bool        auto_ack = false;                 // false=手动 ack，处理完才确认，消息不丢
    int         prefetch = 100;                   // QoS 预取条数，限制未 ack 的消息量
    int         consume_timeout_ms = 1000;        // 单次拉取超时，用于及时响应 Stop()
    int         reconnect_delay_ms = 3000;        // 断线重连间隔

    // ------------------------------------------------------------------
    // 落盘参数（消费到的日志写到哪里）
    // ------------------------------------------------------------------
    std::string log_dir = "log/";                 // 日志目录
    std::string log_basename = "log";             // 文件名前缀

    /**
     * @brief 简单校验配置，返回空串表示合法，否则返回错误原因。
     *
     * 只做最基本的检查，避免明显错误（空地址、非法端口）进入连接流程。
     */
    std::string Validate() const {
        if (host.empty()) return "rabbitmq.host 不能为空";
        if (port <= 0 || port > 65535) return "rabbitmq.port 非法";
        if (vhost.empty()) return "rabbitmq.vhost 不能为空";
        if (user.empty()) return "rabbitmq.user 不能为空";
        if (exchange.empty()) return "rabbitmq.exchange 不能为空";
        if (queue.empty()) return "rabbitmq.queue 不能为空";
        if (prefetch <= 0) return "rabbitmq.prefetch 必须大于 0";
        if (consume_timeout_ms <= 0) return "rabbitmq.consume_timeout_ms 必须大于 0";
        if (reconnect_delay_ms <= 0) return "rabbitmq.reconnect_delay_ms 必须大于 0";
        return std::string();
    }
};

#endif  // CLOG_RABBITMQ_CONFIG_H

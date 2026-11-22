#ifndef CLOG_RABBITMQ_PUBLISHER_H
#define CLOG_RABBITMQ_PUBLISHER_H

/**
 * @file rabbitmq_publisher.h
 * @brief RabbitMQ 日志发布者：把日志发送到消息队列。
 *
 * 用途：
 *   - 业务进程可以把日志 publish 到 MQ，由独立的日志服务器统一消费落盘；
 *   - 也用于本地联调：先 publish 几条，再启动消费者验证链路。
 *
 * 发布方向与 RabbitMQConsumer 对称：
 *   发布者 ──basic.publish──> exchange ──routing_key──> queue
 *
 * 注意：本类是轻量封装，未开启 publisher confirm（发布确认）。
 * 对“日志”这类允许极少数丢失的场景足够；若要求不丢，
 * 可改用 mandatory + confirm 模式。
 */

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

#include "log_message.h"
#include "rabbitmq_config.h"
#include "rabbitmq_detail.h"

class RabbitMQPublisher {
public:
    explicit RabbitMQPublisher(RabbitMQConfig cfg) : cfg_(std::move(cfg)) {}

    ~RabbitMQPublisher() { Close(); }

    RabbitMQPublisher(const RabbitMQPublisher&) = delete;
    RabbitMQPublisher& operator=(const RabbitMQPublisher&) = delete;

    /**
     * @brief 建立连接并声明交换机。
     */
    bool Connect() {
        const std::string verr = cfg_.Validate();
        if (!verr.empty()) return false;
        std::string err;
        return Open(err);
    }

    /**
     * @brief 发布一条原始文本消息。
     *
     * @param routing_key 路由键，例如 "log.app1"。
     * @param body        消息体；通常是一行标准日志。
     * @return true 表示已成功交给内核 socket（不保证 broker 已持久化）。
     */
    bool Publish(const std::string& routing_key, const std::string& body) {
        std::string err;
        if (!EnsureOpen(err)) return false;

        // 设置消息属性：
        //   content_type  = text/plain
        //   delivery_mode = 2（持久化，broker 重启后消息仍在）
        amqp_basic_properties_t props;
        std::memset(&props, 0, sizeof(props));
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
                       AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;

        // 用 amqp_bytes_t 而不是 amqp_cstring_bytes，
        // 避免消息体中出现 '\0' 时被截断。
        amqp_bytes_t body_bytes;
        body_bytes.len = body.size();
        body_bytes.bytes = const_cast<char*>(body.data());

        const int rc = amqp_basic_publish(
            conn_, channel_, amqp_cstring_bytes(cfg_.exchange.c_str()),
            amqp_cstring_bytes(routing_key.c_str()),
            /*mandatory=*/0, /*immediate=*/0, &props, body_bytes);

        if (rc != AMQP_STATUS_OK) {
            // 发送失败通常意味着连接已断，关闭后由下次调用重连。
            Close();
            return false;
        }
        ++published_;
        return true;
    }

    /**
     * @brief 发布一条结构化日志。
     *
     * 内部先按标准格式拼成一行，再走 Publish()。
     */
    bool PublishLog(LogLevel lv, const std::string& source,
                    const std::string& message,
                    const std::string& routing_key = std::string()) {
        LogRecord rec;
        rec.timestamp = NowTimestamp();
        rec.level = lv;
        rec.source = source;
        rec.message = message;
        const std::string line = FormatLogRecord(rec);
        return Publish(routing_key.empty() ? cfg_.routing_key : routing_key, line);
    }

    // 已成功提交的消息数。
    std::uint64_t published() const { return published_; }

private:
    bool Open(std::string& err) {
        if (conn_ != nullptr) return true;
        if (!mq_detail::OpenConnection(cfg_, conn_, channel_, err)) {
            return false;
        }
        if (!mq_detail::DeclareExchange(conn_, channel_, cfg_, err)) {
            mq_detail::CloseConnection(conn_, channel_);
            return false;
        }
        return true;
    }

    bool EnsureOpen(std::string& err) {
        if (conn_ != nullptr) return true;
        return Open(err);
    }

    void Close() {
        if (conn_ == nullptr) return;
        mq_detail::CloseConnection(conn_, channel_);
    }

private:
    RabbitMQConfig          cfg_;
    amqp_connection_state_t conn_ = nullptr;
    amqp_channel_t          channel_ = 1;
    std::uint64_t           published_ = 0;
};

#endif  // CLOG_RABBITMQ_PUBLISHER_H

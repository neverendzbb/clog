#ifndef CLOG_RABBITMQ_DETAIL_H
#define CLOG_RABBITMQ_DETAIL_H

/**
 * @file rabbitmq_detail.h
 * @brief rabbitmq-c 的薄封装：连接、拓扑声明、错误文本。
 *
 * 本文件只放“消费者和发布者都要用”的公共步骤，
 * 避免两处重复编写 amqp_* 调用与错误检查。
 *
 * 依赖：librabbitmq（rabbitmq-c），编译时需 -lrabbitmq。
 */

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <cstdint>
#include <string>

#include "rabbitmq_config.h"

namespace mq_detail {

/**
 * @brief amqp_bytes_t -> std::string。
 *
 * 注意 amqp_bytes_t.bytes 可能为空指针，len 可能为 0，
 * 直接构造 std::string(nullptr, 0) 属于未定义行为，必须先判空。
 */
inline std::string BytesToStr(const amqp_bytes_t& b) {
    if (b.bytes == nullptr || b.len == 0) return std::string();
    return std::string(static_cast<const char*>(b.bytes), b.len);
}

/**
 * @brief 把 amqp_rpc_reply_t 转成可读的错误文本。
 *
 * 区分三类响应：
 *   - NORMAL            ：成功；
 *   - LIBRARY_EXCEPTION ：客户端/网络错误，带 library_error；
 *   - SERVER_EXCEPTION  ：broker 返回错误，尽量解出 reply_code / reply_text。
 */
inline std::string ReplyText(const amqp_rpc_reply_t& r) {
    if (r.reply_type == AMQP_RESPONSE_NORMAL) return "ok";

    if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        return std::string("library_error=") + std::to_string(r.library_error) +
               " (" + amqp_error_string2(r.library_error) + ")";
    }

    // AMQP_RESPONSE_SERVER_EXCEPTION
    if (r.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
        auto* c = static_cast<amqp_channel_close_t*>(r.reply.decoded);
        if (c != nullptr) {
            return "channel.close code=" + std::to_string(c->reply_code) +
                   " text=" + BytesToStr(c->reply_text);
        }
    } else if (r.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
        auto* c = static_cast<amqp_connection_close_t*>(r.reply.decoded);
        if (c != nullptr) {
            return "connection.close code=" + std::to_string(c->reply_code) +
                   " text=" + BytesToStr(c->reply_text);
        }
    }
    return "server_exception method_id=" + std::to_string(r.reply.id);
}

/**
 * @brief 创建连接、打开 socket、登录、打开 channel。
 *
 * 失败时会自动销毁已创建的连接，保证 conn 为 nullptr，调用方无需清理。
 *
 * @param[in]  cfg     连接配置。
 * @param[out] conn    成功时持有连接句柄。
 * @param[in]  channel 使用的 channel 编号（1 即可）。
 * @param[out] err     失败原因。
 * @return true 表示连接与 channel 均就绪。
 */
inline bool OpenConnection(const RabbitMQConfig& cfg,
                           amqp_connection_state_t& conn,
                           amqp_channel_t channel,
                           std::string& err) {
    conn = amqp_new_connection();
    if (conn == nullptr) {
        err = "amqp_new_connection 失败（内存不足）";
        return false;
    }

    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    if (socket == nullptr) {
        err = "amqp_tcp_socket_new 失败";
        amqp_destroy_connection(conn);
        conn = nullptr;
        return false;
    }

    const int rc = amqp_socket_open(socket, cfg.host.c_str(), cfg.port);
    if (rc != AMQP_STATUS_OK) {
        err = std::string("socket_open 失败: ") + amqp_error_string2(rc);
        amqp_destroy_connection(conn);
        conn = nullptr;
        return false;
    }

    const int max_channels =
        cfg.channel_max > 0 ? cfg.channel_max : AMQP_DEFAULT_MAX_CHANNELS;
    const int max_frame =
        cfg.frame_max > 0 ? cfg.frame_max : AMQP_DEFAULT_FRAME_SIZE;

    // 变参：AMQP_SASL_METHOD_PLAIN 后面必须紧跟 user、password。
    amqp_rpc_reply_t r = amqp_login(conn, cfg.vhost.c_str(), max_channels,
                                    max_frame, cfg.heartbeat,
                                    AMQP_SASL_METHOD_PLAIN, cfg.user.c_str(),
                                    cfg.password.c_str());
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        err = "login 失败: " + ReplyText(r);
        amqp_destroy_connection(conn);
        conn = nullptr;
        return false;
    }

    // 防御性设置：所有 RPC（channel_close / queue_declare 等）最多等待 3 秒。
    // 否则 broker 半关闭连接时，客户端可能永久阻塞在等待回包上。
    struct timeval rpc_timeout;
    rpc_timeout.tv_sec = 3;
    rpc_timeout.tv_usec = 0;
    amqp_set_rpc_timeout(conn, &rpc_timeout);

    // 打开逻辑通道；绝大多数场景 channel 1 足够。
    amqp_channel_open(conn, channel);
    r = amqp_get_rpc_reply(conn);
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        err = "channel_open 失败: " + ReplyText(r);
        amqp_destroy_connection(conn);
        conn = nullptr;
        return false;
    }

    return true;
}

/**
 * @brief 声明交换机（幂等：已存在且参数一致时不会报错）。
 */
inline bool DeclareExchange(amqp_connection_state_t conn, amqp_channel_t channel,
                            const RabbitMQConfig& cfg, std::string& err) {
    amqp_exchange_declare(
        conn, channel, amqp_cstring_bytes(cfg.exchange.c_str()),
        amqp_cstring_bytes(cfg.exchange_type.c_str()),
        /*passive=*/0,
        /*durable=*/cfg.exchange_durable ? 1 : 0,
        /*auto_delete=*/cfg.exchange_auto_delete ? 1 : 0,
        /*internal=*/0, amqp_empty_table);

    amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        err = "exchange_declare 失败: " + ReplyText(r);
        return false;
    }
    return true;
}

/**
 * @brief 声明队列（幂等）。
 */
inline bool DeclareQueue(amqp_connection_state_t conn, amqp_channel_t channel,
                         const RabbitMQConfig& cfg, std::string& err) {
    amqp_queue_declare(conn, channel, amqp_cstring_bytes(cfg.queue.c_str()),
                       /*passive=*/0,
                       /*durable=*/cfg.queue_durable ? 1 : 0,
                       /*exclusive=*/cfg.queue_exclusive ? 1 : 0,
                       /*auto_delete=*/cfg.queue_auto_delete ? 1 : 0,
                       amqp_empty_table);

    amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        err = "queue_declare 失败: " + ReplyText(r);
        return false;
    }
    return true;
}

/**
 * @brief 把队列绑定到交换机。
 */
inline bool BindQueue(amqp_connection_state_t conn, amqp_channel_t channel,
                      const RabbitMQConfig& cfg, std::string& err) {
    amqp_queue_bind(conn, channel, amqp_cstring_bytes(cfg.queue.c_str()),
                    amqp_cstring_bytes(cfg.exchange.c_str()),
                    amqp_cstring_bytes(cfg.routing_key.c_str()),
                    amqp_empty_table);

    amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        err = "queue_bind 失败: " + ReplyText(r);
        return false;
    }
    return true;
}

/**
 * @brief 设置 QoS 预取条数。
 *
 * 限制“已投递但未 ack”的消息数量：消费者处理得慢时，
 * broker 不会把海量消息一次性压到客户端内存里。
 */
inline bool SetQos(amqp_connection_state_t conn, amqp_channel_t channel,
                   std::uint16_t prefetch, std::string& err) {
    amqp_basic_qos(conn, channel, /*prefetch_size=*/0, prefetch,
                   /*global=*/0);

    amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
    if (r.reply_type != AMQP_RESPONSE_NORMAL) {
        err = "basic_qos 失败: " + ReplyText(r);
        return false;
    }
    return true;
}

/**
 * @brief 优雅关闭连接：关 channel、关连接、释放句柄。
 *
 * 仅用于“连接仍然健康”的场景（例如正常退出）。
 * 若连接已经出错（broker 已发送 close 帧），请改用 AbortConnection()，
 * 否则 amqp_channel_close() 可能在等待一个永远不会到来的回包。
 */
inline void CloseConnection(amqp_connection_state_t& conn,
                            amqp_channel_t channel) {
    if (conn == nullptr) return;
    amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    conn = nullptr;
}

/**
 * @brief 直接销毁连接，不发送任何 close RPC。
 *
 * 用于连接已经异常（断网、broker 主动 close、协议错误）的场景：
 * 此时再调用 channel_close/connection_close 可能阻塞，
 * 直接销毁句柄后重连是最安全的做法。
 */
inline void AbortConnection(amqp_connection_state_t& conn) {
    if (conn == nullptr) return;
    amqp_destroy_connection(conn);
    conn = nullptr;
}

}  // namespace mq_detail

#endif  // CLOG_RABBITMQ_DETAIL_H

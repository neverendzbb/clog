/**
 * @file mq_consumer_main.cpp
 * @brief 日志服务器示例：从 RabbitMQ 消费日志并落盘到本地文件。
 *
 * 运行：
 *   ./mq_consumer
 * 环境变量（均可选，括号内为默认值）：
 *   MQ_HOST(127.0.0.1) MQ_PORT(5672) MQ_VHOST(/) MQ_USER(guest)
 *   MQ_PASSWORD(guest) MQ_EXCHANGE(log.exchange) MQ_QUEUE(log.queue)
 *   MQ_ROUTING_KEY(log.#) MQ_LOG_DIR(log/) MQ_LOG_BASENAME(log)
 *
 * 退出：Ctrl+C 或 kill -TERM，会优雅停止并打印最终统计。
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "rabbitmq_consumer.h"

namespace {

// 信号处理函数只做“置标志”，这是异步信号安全的最小操作。
volatile std::sig_atomic_t g_signaled = 0;

void OnSignal(int /*sig*/) { g_signaled = 1; }

std::string EnvOr(const char* key, const std::string& def) {
    const char* v = std::getenv(key);
    return (v != nullptr && *v != '\0') ? std::string(v) : def;
}

int EnvIntOr(const char* key, int def) {
    const char* v = std::getenv(key);
    if (v == nullptr || *v == '\0') return def;
    try {
        return std::stoi(v);
    } catch (...) {
        return def;
    }
}

}  // namespace

int main() {
    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);

    // 从环境变量读取配置，未设置则使用结构体默认值。
    RabbitMQConfig cfg;
    cfg.host         = EnvOr("MQ_HOST", cfg.host);
    cfg.port         = EnvIntOr("MQ_PORT", cfg.port);
    cfg.vhost        = EnvOr("MQ_VHOST", cfg.vhost);
    cfg.user         = EnvOr("MQ_USER", cfg.user);
    cfg.password     = EnvOr("MQ_PASSWORD", cfg.password);
    cfg.exchange     = EnvOr("MQ_EXCHANGE", cfg.exchange);
    cfg.queue        = EnvOr("MQ_QUEUE", cfg.queue);
    cfg.routing_key  = EnvOr("MQ_ROUTING_KEY", cfg.routing_key);
    cfg.log_dir      = EnvOr("MQ_LOG_DIR", cfg.log_dir);
    cfg.log_basename = EnvOr("MQ_LOG_BASENAME", cfg.log_basename);

    std::cout << "[consumer] broker=" << cfg.host << ":" << cfg.port
              << " vhost=" << cfg.vhost << " exchange=" << cfg.exchange
              << " queue=" << cfg.queue << " routing_key=" << cfg.routing_key
              << " log_dir=" << cfg.log_dir << "\n";

    RabbitMQConsumer consumer(cfg);

    // 消费循环放到工作线程；主线程只负责等待信号和打印统计。
    std::atomic<bool> done{false};
    std::thread worker([&consumer, &done]() {
        consumer.Run();
        done.store(true, std::memory_order_relaxed);
    });

    // 每 2 秒输出一次统计，便于观察消费进度。
    while (g_signaled == 0 && !done.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        std::cout << "[consumer] received=" << consumer.received()
                  << " written=" << consumer.written()
                  << " ack_failed=" << consumer.ack_failed()
                  << " reconnects=" << consumer.reconnects() << std::endl;
    }

    consumer.Stop();
    if (worker.joinable()) worker.join();

    std::cout << "[consumer] 已退出，最终统计: received=" << consumer.received()
              << " written=" << consumer.written()
              << " ack_failed=" << consumer.ack_failed()
              << " reconnects=" << consumer.reconnects() << "\n";
    return 0;
}

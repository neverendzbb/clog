/**
 * @file mq_publisher_main.cpp
 * @brief 日志发布者示例：向 RabbitMQ 发送测试日志，用于联调。
 *
 * 用法：
 *   ./mq_publisher [条数] [间隔毫秒]
 *   默认：10 条，每条间隔 100ms。
 *
 * 演示两种消息：
 *   1. PublishLog()：结构化日志，带时间戳/级别/来源；
 *   2. Publish()   ：裸文本，消费者会用当前时间 + INFO 自动包装。
 *
 * 配合 ./mq_consumer 使用即可验证「发布 -> 队列 -> 消费 -> 落盘」全链路。
 */

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "rabbitmq_publisher.h"

int main(int argc, char** argv) {
    const int count = (argc > 1) ? std::atoi(argv[1]) : 10;
    const int interval_ms = (argc > 2) ? std::atoi(argv[2]) : 100;

    RabbitMQConfig cfg;
    // 发布到与消费者一致的 exchange，routing_key 位于 "log.#" 匹配范围内。
    cfg.routing_key = "log.app1";

    RabbitMQPublisher pub(cfg);
    if (!pub.Connect()) {
        std::cerr << "[publisher] 连接 RabbitMQ 失败，请检查 broker 是否可用\n";
        return 1;
    }

    for (int i = 1; i <= count; ++i) {
        // 演示 1：结构化日志
        pub.PublishLog(LogLevel::INFO, "mq_publisher:main",
                       "第 " + std::to_string(i) + " 条结构化日志", "log.app1");

        // 演示 2：每 5 条发一条裸文本，验证消费者的兜底包装逻辑
        if (i % 5 == 0) {
            pub.Publish("log.app1",
                        "裸消息 #" + std::to_string(i) + "（未格式化）");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }

    std::cout << "[publisher] 已发送 " << pub.published() << " 条消息\n";
    return 0;
}

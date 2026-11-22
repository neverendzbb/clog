# ============================================================================
# clog —— 带 RabbitMQ 消费能力的 C++ 日志库
# ============================================================================
# 目标：
#   make            构建全部（main / mq_consumer / mq_publisher）
#   make main       仅构建本地日志示例（不依赖 RabbitMQ）
#   make mq         构建 RabbitMQ 消费者与发布者
#   make clean      清理产物
#
# 依赖：
#   - g++ 支持 C++17
#   - librabbitmq-dev（rabbitmq-c 头文件与库）
#       sudo apt-get install librabbitmq-dev
#   - libssl-dev（rabbitmq-c 的 TLS 依赖，链接时通常需要）
#       sudo apt-get install libssl-dev
# ============================================================================

CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pthread
# rabbitmq-c 依赖 OpenSSL；-pthread 用于 std::thread
MQ_LIBS  := -lrabbitmq -lssl -lcrypto -pthread

.PHONY: all mq clean

all: main mq_consumer mq_publisher

# 本地日志示例：把日志直接写入文件（不经过 MQ）
main: main.cpp \
      LogLevel.h log_message.h logfile.h async_logger.h printlog.h
	$(CXX) $(CXXFLAGS) main.cpp -o $@

mq_consumer: mq_consumer_main.cpp \
             rabbitmq_consumer.h rabbitmq_detail.h rabbitmq_config.h \
             async_logger.h log_message.h logfile.h LogLevel.h
	$(CXX) $(CXXFLAGS) mq_consumer_main.cpp -o $@ $(MQ_LIBS)

mq_publisher: mq_publisher_main.cpp \
              rabbitmq_publisher.h rabbitmq_detail.h rabbitmq_config.h \
              log_message.h LogLevel.h
	$(CXX) $(CXXFLAGS) mq_publisher_main.cpp -o $@ $(MQ_LIBS)

mq: mq_consumer mq_publisher

clean:
	rm -f main mq_consumer mq_publisher

#ifndef CLOG_ASYNC_LOGGER_H
#define CLOG_ASYNC_LOGGER_H

/**
 * @file async_logger.h
 * @brief 异步日志器：业务线程只入队，后台线程批量落盘。
 *
 * 为什么需要异步？
 *   直接写文件（fwrite + fflush）是系统调用，若每条日志都同步写，
 *   业务线程会被磁盘 IO 拖慢，甚至在高并发下出现明显抖动。
 *
 * 本类的做法：
 *   1. append() 在锁保护下把日志放进行内队列，随即返回；
 *   2. 后台线程在以下任一条件满足时被唤醒并批量处理：
 *        - 队列条数达到 kBatchMax（默认 200）；
 *        - 距上次处理超过 kFlushInterval（默认 200ms）；
 *        - 收到退出信号（析构）；
 *   3. 一次处理把整批日志拼成一个字符串，只调用一次 write + flush，
 *      把系统调用次数从 N 降到 1。
 *
 * 队列上限与背压：
 *   队列超过 kMaxQueueSize（默认 10 万条）时，丢弃最旧的一条。
 *   这是“有损但保命”的策略——宁可丢日志，也不让内存无限增长拖垮进程。
 *   同时用 dropped_ 记录丢弃条数，便于监控。
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "logfile.h"

class AsyncLogger {
public:
    // 队列硬上限：达到后丢弃最旧日志，防止内存失控。
    static constexpr std::size_t kMaxQueueSize = 100000;
    // 批量阈值：攒够这么多条立刻唤醒后台线程。
    static constexpr std::size_t kBatchMax = 200;
    // 定时阈值：即使没攒够一批，也最多等这么久就落盘一次。
    static constexpr std::chrono::milliseconds kFlushInterval{200};

    /**
     * @brief 构造并启动后台写线程。
     * @param dir      日志目录（透传给 logfile）。
     * @param basename 文件名前缀。
     */
    explicit AsyncLogger(std::string dir = "log/", std::string basename = "log")
        : dir_(std::move(dir)), basename_(std::move(basename)) {
        // logfile 是单例；这里拿到的指针在进程生命周期内始终有效。
        file_ = &logfile::Instance(dir_, basename_);
        worker_ = std::thread(&AsyncLogger::Run, this);
    }

    /**
     * @brief 析构：通知后台线程退出，并等待队列排空。
     *
     * 顺序很重要：先置退出标志、再唤醒，最后 join。
     * 后台线程的循环条件是「退出标志为真 且 队列为空」，
     * 因此会把剩余日志写完再退出，不会丢尾部数据。
     */
    ~AsyncLogger() {
        stop_flag_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    /**
     * @brief 线程安全入队（非阻塞）。
     *
     * 只做字符串移动和队列操作，不涉及任何文件 IO，
     * 因此对业务线程的延迟影响极小。
     */
    void append(std::string msg) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            // 背压：队列满时丢最旧的一条（有损，但保护内存）
            if (queue_.size() >= kMaxQueueSize) {
                queue_.pop_front();
                dropped_.fetch_add(1, std::memory_order_relaxed);
            }
            queue_.push_back(std::move(msg));
        }
        // 通知放在锁外，减少后台线程被唤醒后立即抢锁的竞争。
        cv_.notify_one();
    }

    /**
     * @brief 进程级单例。
     *
     * 与 logfile::Instance 一样，参数只在第一次调用时生效。
     */
    static AsyncLogger& Instance(std::string dir = "log/",
                                 std::string basename = "log") {
        static AsyncLogger inst(std::move(dir), std::move(basename));
        return inst;
    }

    // 当前队列中待落盘的日志条数（监控用）。
    std::size_t pending() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return queue_.size();
    }

    // 因队列满被丢弃的日志条数（监控用）。
    std::uint64_t dropped() const {
        return dropped_.load(std::memory_order_relaxed);
    }

private:
    // 后台线程主循环。
    void Run() {
        std::unique_lock<std::mutex> lk(mutex_);

        // 退出条件：收到停止信号“且”队列已排空。
        while (!stop_flag_.load(std::memory_order_relaxed) || !queue_.empty()) {
            // 最多等 kFlushInterval；期间若攒够一批或收到退出信号则提前醒。
            cv_.wait_for(lk, kFlushInterval, [this]() {
                return stop_flag_.load(std::memory_order_relaxed) ||
                       queue_.size() >= kBatchMax;
            });

            // 用 swap 把整批“偷”出来，随后立刻释放锁，
            // 让业务线程可以继续入队，不被磁盘 IO 阻塞。
            std::deque<std::string> batch;
            batch.swap(queue_);
            lk.unlock();

            if (file_ != nullptr && !batch.empty()) {
                // 预估总长度，减少拼接时的扩容次数。
                std::size_t total = 0;
                for (const auto& line : batch) total += line.size() + 1;

                std::string buf;
                buf.reserve(total + 1);
                for (const auto& line : batch) {
                    buf += line;
                    buf += '\n';  // 每条日志占一行
                }

                // 一次 write + 一次 flush，吞吐远高于逐条写入。
                file_->write(buf);
                file_->flush();
            }

            lk.lock();  // 回到循环顶部前重新加锁
        }
    }

private:
    logfile*              file_ = nullptr;  // 指向 logfile 单例
    std::string           dir_;
    std::string           basename_;

    std::thread           worker_;
    mutable std::mutex    mutex_;            // 保护 queue_，并配合 cv_
    std::condition_variable cv_;
    std::deque<std::string> queue_;          // 待落盘日志
    std::atomic<bool>     stop_flag_{false}; // 退出标志
    std::atomic<std::uint64_t> dropped_{0};  // 丢弃计数
};

#endif  // CLOG_ASYNC_LOGGER_H

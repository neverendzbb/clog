#ifndef ASYNC_LOGGER_H
#define ASYNC_LOGGER_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <string>
#include <cstdio>
#include <atomic>
#include <utility>
#include <chrono>
#include "logfile.h"
class AsyncLogger {
public:
    AsyncLogger(std::string ddir="log/",std::string bbasename="log")
        : fp(nullptr), exit_flag(false), lastfflush(std::chrono::system_clock::now()),
        dir(ddir),basename(bbasename)
    {
        fp = &(logfile::Instance(ddir,bbasename));
        worker = std::thread(&AsyncLogger::run, this);
    }

    ~AsyncLogger()
    {
        exit_flag.store(true, std::memory_order_relaxed);
        cv.notify_all();
        if (worker.joinable()) worker.join();
    }

    // Non-copyable
    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    void append(std::string msg)
    {
        std::lock_guard<std::mutex> lk(m);
        queue.push_back(std::move(msg));
        // simple backpressure: drop oldest when queue too large
        if (queue.size() > 10000) queue.pop_front();
        cv.notify_one();
    }

    static AsyncLogger& Instance(std::string dir="log/",std::string basename="log")
    {
        static AsyncLogger inst(dir,basename);
        return inst;
    }

private:
    void run()
    {
        std::unique_lock<std::mutex> lk(m);
        // 批量阈值200，定时200ms强制刷盘
        constexpr size_t BATCH_MAX = 200;
        constexpr auto FLUSH_DUR = std::chrono::milliseconds(200);

        while (!exit_flag.load(std::memory_order_relaxed) || !queue.empty())
        {
            // 最多等待10s，避免空队列忙等
            cv.wait_for(lk, FLUSH_DUR, [&]() {
                return exit_flag.load(std::memory_order_relaxed) || queue.size() >= BATCH_MAX;
            });

            // 一次性交换队列，快速释放锁
            std::deque<std::string> batch;
            std::swap(batch, queue);
            lk.unlock();

            if (fp && !batch.empty())
            {
                std::string buf;
                // 预分配内存减少扩容
                buf.reserve(batch.size() * 256);
                for (auto& line : batch)
                {
                    buf += line;
                    buf += '\n';
                }
                fp->write(buf);
            }

            lk.lock();
            lastfflush = std::chrono::system_clock::now();
        }
    }

    logfile* fp;
    std::string dir="log/";
    std::string basename="log";
    std::thread worker;
    std::mutex m;
    std::condition_variable cv;
    std::deque<std::string> queue;
    std::atomic<bool> exit_flag;
    std::chrono::system_clock::time_point lastfflush;
};

#endif
#ifndef ASYNC_LOGGER_H
#define ASYNC_LOGGER_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <string>
#include <cstdio>
#include <atomic>
#include <memory>
#include <utility>

class AsyncLogger {
public:
    AsyncLogger(const std::string& path = "log.txt")
        : fp(nullptr), exit_flag(false)
    {
        fp = std::fopen(path.c_str(), "a");
        worker = std::thread(&AsyncLogger::run, this);
    }

    ~AsyncLogger()
    {
        exit_flag.store(true, std::memory_order_relaxed);
        cv.notify_all();
        if (worker.joinable()) worker.join();
        if (fp) std::fclose(fp);
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

    static AsyncLogger& Instance()
    {
        static AsyncLogger inst;
        return inst;
    }

private:
    void run()
    {
        std::unique_lock<std::mutex> lk(m);
        while (!exit_flag.load(std::memory_order_relaxed) || !queue.empty()) {
            cv.wait(lk, [&]() { return exit_flag.load(std::memory_order_relaxed) || !queue.empty(); });
            while (!queue.empty()) {
                std::string s = std::move(queue.front());
                queue.pop_front();
                lk.unlock();
                if (fp) {
                    std::fputs(s.c_str(), fp);
                    std::fputs("\n", fp);
                    std::fflush(fp);
                }
                lk.lock();
            }
        }
    }

    FILE* fp;
    std::thread worker;
    std::mutex m;
    std::condition_variable cv;
    std::deque<std::string> queue;
    std::atomic<bool> exit_flag;
};

#endif

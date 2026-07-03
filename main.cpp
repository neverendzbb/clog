#include "printlog.h"
#include <thread>
#include <vector>

int main()
{
    const int thread_count = 10;
    const int logs_per_thread = 5;
    std::vector<std::thread> workers;
    workers.reserve(thread_count);

    for (int i = 0; i < thread_count; ++i) {
        workers.emplace_back([i]() {
            for (int j = 0; j < logs_per_thread; ++j) {
                LOG_INFO("线程 %d 第 %d 条日志，端口 %d", i, j, 8080);
            }
        });
    }

    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    LOG_ERROR("多线程测试结束，线程数=%d，总日志=%d", thread_count, thread_count * logs_per_thread);
    return 0;
}
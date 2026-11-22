#ifndef CLOG_PRINTLOG_H
#define CLOG_PRINTLOG_H

/**
 * @file printlog.h
 * @brief 对外日志接口：LOG_DEBUG / LOG_INFO / ... 宏。
 *
 * 调用关系：
 *   LOG_INFO("x=%d", 1)
 *     └─> PrintLog(level, file, line, fmt, ...)
 *           ├─ 格式化正文（支持任意长度，不截断）
 *           ├─ 采集时间戳、级别、来源（file:line）
 *           └─> AsyncLogger::append(标准日志行)   // 非阻塞入队
 *                 └─> 后台线程批量落盘到 logfile
 *
 * 这样业务代码无需关心文件、线程、滚动等细节。
 */

#include <cstdarg>
#include <cstdio>
#include <string>
#include <utility>

#include "LogLevel.h"
#include "async_logger.h"
#include "log_message.h"

/**
 * @brief 把可变参数格式化为 std::string。
 *
 * 先用 vsnprintf(nullptr, 0, ...) 探测需要的长度，
 * 再按实际长度分配缓冲区，避免固定缓冲区（如 1024 字节）截断长日志。
 *
 * @param fmt printf 风格格式串。
 * @param ap  已 va_start 的参数列表；本函数只读取，不负责 va_end。
 */
inline std::string FormatVariadic(const char* fmt, va_list ap) {
    if (fmt == nullptr) return std::string();

    // 1) 探测长度（vsnprintf 返回“若缓冲区足够大”将写入的字符数，不含 '\0'）
    va_list ap_probe;
    va_copy(ap_probe, ap);
    const int need = std::vsnprintf(nullptr, 0, fmt, ap_probe);
    va_end(ap_probe);
    if (need <= 0) return std::string();

    // 2) 按实际长度分配，并写入。
    //    std::string 在 C++11 起保证 data()[size()] 是 '\0'，
    //    因此写入 size()+1 字节（含结尾 '\0'）是安全的。
    std::string out(static_cast<std::size_t>(need), '\0');
    std::vsnprintf(&out[0], out.size() + 1, fmt, ap);
    return out;
}

/**
 * @brief 日志写入的核心函数（一般不直接调用，使用 LOG_* 宏）。
 *
 * @param lv   日志级别。
 * @param file 源文件名（由宏传入 __FILE__）。
 * @param line 源文件行号（由宏传入 __LINE__）。
 * @param fmt  printf 风格格式串，后接可变参数。
 */
inline void PrintLog(LogLevel lv, const char* file, int line,
                     const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::string content = FormatVariadic(fmt, ap);
    va_end(ap);

    LogRecord rec;
    rec.timestamp = NowTimestamp();
    rec.level     = lv;
    rec.source    = std::string(file != nullptr ? file : "?") + ":" +
                    std::to_string(line);
    rec.message   = std::move(content);

    // 只入队、不落盘：真正的写文件由 AsyncLogger 的后台线程完成。
    AsyncLogger::Instance().append(FormatLogRecord(rec));
}

// 便捷宏：自动带上级别、文件名、行号。
#define LOG_DEBUG(...) PrintLog(LogLevel::DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  PrintLog(LogLevel::INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  PrintLog(LogLevel::WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) PrintLog(LogLevel::ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_FATAL(...) PrintLog(LogLevel::FATAL, __FILE__, __LINE__, __VA_ARGS__)

#endif  // CLOG_PRINTLOG_H

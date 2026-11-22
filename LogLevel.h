#ifndef CLOG_LOG_LEVEL_H
#define CLOG_LOG_LEVEL_H

#include <cctype>
#include <string>

/**
 * @file LogLevel.h
 * @brief 日志级别定义与字符串互转。
 *
 * 级别采用「由轻到重」的整数枚举，数值越大越严重。
 * 这样可以用最简单的数值比较做过滤，例如：
 *   if (lv < LogLevel::WARN) return;   // 只记录 WARN 及以上
 */

// 日志级别。
// 说明：
//   DEBUG —— 调试信息，开发/排障时使用，通常量最大；
//   INFO  —— 正常业务流程信息，线上默认级别；
//   WARN  —— 警告，不影响主流程，但需要人工关注；
//   ERROR —— 错误，某个功能已经失败，但进程还能继续；
//   FATAL —— 致命错误，通常意味着进程无法继续，往往伴随退出。
enum class LogLevel : int {
    DEBUG = 0,
    INFO  = 1,
    WARN  = 2,
    ERROR = 3,
    FATAL = 4
};

/**
 * @brief 级别 -> 大写字符串。
 *
 * 用于日志落盘和经消息队列传输，保证两端格式一致。
 * 传入非法枚举值时返回 "UNKNOWN"，避免写出空字段。
 */
inline const char* LevelToStr(LogLevel lv) {
    switch (lv) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        default:              return "UNKNOWN";
    }
}

/**
 * @brief 字符串 -> 级别（大小写不敏感）。
 *
 * 供“从消息队列反序列化日志”时使用：生产端可能写 "info"、"Warn" 等。
 * @param s        待解析字符串，允许前后带空白。
 * @param fallback 解析失败时的兜底级别，默认 INFO。
 *
 * 解析失败也永远返回一个合法级别，调用方无需再做判空。
 */
inline LogLevel LevelFromStr(const std::string& s,
                             LogLevel fallback = LogLevel::INFO) {
    // 先去掉首尾空白并转大写，简化后续比较
    std::string u;
    u.reserve(s.size());
    for (char c : s) {
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        u.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    if (u == "DEBUG")                 return LogLevel::DEBUG;
    if (u == "INFO")                  return LogLevel::INFO;
    if (u == "WARN" || u == "WARNING") return LogLevel::WARN;
    if (u == "ERROR")                 return LogLevel::ERROR;
    if (u == "FATAL" || u == "CRITICAL") return LogLevel::FATAL;
    return fallback;
}

#endif  // CLOG_LOG_LEVEL_H

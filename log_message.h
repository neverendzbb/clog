#ifndef CLOG_LOG_MESSAGE_H
#define CLOG_LOG_MESSAGE_H

/**
 * @file log_message.h
 * @brief 日志记录结构、格式化与解析。
 *
 * 本文件是「本地落盘」与「消息队列传输」之间的公共协议层：
 *   - FormatLogRecord() 把一条 LogRecord 拼成标准文本行；
 *   - ParseLogRecord() 把标准文本行还原成 LogRecord；
 *   - 只要两端使用同一套函数，本地日志和经 MQ 汇聚的日志格式就完全一致。
 *
 * 标准行格式（一行，不含行尾换行）：
 *   [2026-11-11 23:08:34] [INFO] main.cpp:42 | hello world
 *   └─ 时间戳 ─────────┘ └级别┘ └来源─────┘ └正文─────┘
 */

#include <ctime>
#include <string>

#include "LogLevel.h"

// 一条日志记录的内存表示。
struct LogRecord {
    std::string timestamp;  // 本地时间，形如 "2026-11-11 23:08:34"
    LogLevel    level;      // 级别
    std::string source;     // 来源，如 "main.cpp:42" 或 "mq:log.queue"
    std::string message;    // 正文，允许包含换行（例如堆栈）
};

/**
 * @brief 线程安全地获取当前本地时间字符串。
 *
 * 使用 localtime_r / localtime_s 而不是 localtime：
 * localtime 返回的是静态缓冲区，多线程并发调用会互相覆盖。
 *
 * @param fmt strftime 格式，默认 "%Y-%m-%d %H:%M:%S"。
 */
inline std::string NowTimestamp(const char* fmt = "%Y-%m-%d %H:%M:%S") {
    std::time_t now = std::time(nullptr);
    std::tm tm_now{};
#if defined(_WIN32)
    localtime_s(&tm_now, &now);
#else
    localtime_r(&now, &tm_now);  // POSIX 线程安全版本
#endif
    char buf[64] = {0};
    std::strftime(buf, sizeof(buf), fmt, &tm_now);
    return std::string(buf);
}

/**
 * @brief 把一条日志记录拼成标准文本行（不含末尾换行）。
 *
 * 预分配缓冲区，避免拼接过程中的多次扩容。
 */
inline std::string FormatLogRecord(const LogRecord& r) {
    std::string out;
    // 经验值：固定前缀约 40 字节 + 来源 + 正文
    out.reserve(48 + r.source.size() + r.message.size());
    out += '[';
    out += r.timestamp;
    out += "] [";
    out += LevelToStr(r.level);
    out += "] ";
    out += r.source;
    out += " | ";
    out += r.message;
    return out;
}

/**
 * @brief 解析一行标准日志。
 *
 * 规则与 FormatLogRecord() 严格对称：
 *   '[' 时间戳 ']' ' ' '[' 级别 ']' ' ' 来源 " | " 正文
 *
 * 任何一段缺失都返回 false，由调用方决定如何降级处理
 * （例如把整行当作“裸消息”重新包装）。
 *
 * @param line 输入行（不应包含行尾换行）。
 * @param out  解析成功时填充。
 * @return true 表示解析成功。
 */
inline bool ParseLogRecord(const std::string& line, LogRecord& out) {
    // 1) 时间戳：'[' ... ']'
    if (line.size() < 2 || line[0] != '[') return false;
    const std::size_t p1 = line.find(']', 1);
    if (p1 == std::string::npos) return false;

    // 2) 级别：后面的 '[' ... ']'
    const std::size_t p2 = line.find('[', p1 + 1);
    if (p2 == std::string::npos) return false;
    const std::size_t p3 = line.find(']', p2 + 1);
    if (p3 == std::string::npos) return false;

    // 3) 来源与正文之间用 " | " 分隔
    const std::size_t p4 = line.find(" | ", p3 + 1);
    if (p4 == std::string::npos) return false;

    out.timestamp = line.substr(1, p1 - 1);
    out.level     = LevelFromStr(line.substr(p2 + 1, p3 - p2 - 1));

    // 来源字段可能带一个前导空格（格式是 "] source"），需要去掉
    std::string src = line.substr(p3 + 1, p4 - (p3 + 1));
    std::size_t b = src.find_first_not_of(' ');
    out.source = (b == std::string::npos) ? std::string() : src.substr(b);

    // 正文：分隔符 " | " 之后的所有内容（可含换行）
    out.message = line.substr(p4 + 3);
    return true;
}

/**
 * @brief 把消息队列收到的消息体规范化为“可直接落盘的一行”。
 *
 * 处理策略（优先级从高到低）：
 *   1. 去掉行尾的 \r\n，避免与 AsyncLogger 自动补的 '\n' 叠加；
 *   2. 若首行已是标准日志格式，说明生产端已格式化，原样保留
 *      （尊重生产端的时间戳与级别，便于跨机器排查）；
 *   3. 否则视为裸消息，用「当前时间 + INFO + 来源标签」包装。
 *
 * @param body        MQ 消息体。
 * @param source_tag  裸消息的来源标签，例如 "mq:log.queue"。
 * @return 空串表示 body 去掉空白后为空，调用方应跳过。
 */
inline std::string NormalizeMqBody(const std::string& body,
                                   const std::string& source_tag) {
    // 1) 去掉尾部换行
    std::string s = body;
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) {
        s.pop_back();
    }
    if (s.empty()) return std::string();

    // 2) 首行能解析 -> 认为生产端已格式化，整段原样保留
    const std::size_t nl = s.find('\n');
    const std::string first_line =
        (nl == std::string::npos) ? s : s.substr(0, nl);
    LogRecord probe;
    if (ParseLogRecord(first_line, probe)) {
        return s;
    }

    // 3) 裸消息：统一补上时间戳、级别与来源
    LogRecord rec;
    rec.timestamp = NowTimestamp();
    rec.level     = LogLevel::INFO;
    rec.source    = source_tag;
    rec.message   = s;
    return FormatLogRecord(rec);
}

#endif  // CLOG_LOG_MESSAGE_H

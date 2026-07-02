#ifndef SIMPLE_LOG_H
#define SIMPLE_LOG_H

#include "LogLevel.h"
#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <sstream>
#include "logfile.h"
inline void PrintLog(LogLevel lv, const char* file, int line, const char* fmt, ...)
{
    time_t now = time(nullptr);
    struct tm local_tm;

#if defined(_MSC_VER)
    localtime_s(&local_tm, &now);
#elif defined(__unix__) || defined(__APPLE__)
    localtime_r(&now, &local_tm);
#else
    struct tm* tmp = std::localtime(&now);
    if (tmp) {
        local_tm = *tmp;
    } else {
        std::memset(&local_tm, 0, sizeof(local_tm));
    }
#endif

    char time_buf[64];
    std::snprintf(time_buf, sizeof(time_buf), "[%04d-%02d-%02d %02d:%02d:%02d]",
        local_tm.tm_year + 1900,
        local_tm.tm_mon + 1,
        local_tm.tm_mday,
        local_tm.tm_hour,
        local_tm.tm_min,
        local_tm.tm_sec);

    char content[1024];
    va_list ap;
    va_start(ap, fmt);
    int written = std::vsnprintf(content, sizeof(content), fmt, ap);
    va_end(ap);

    if (written < 0) {
        content[0] = '\0';
    }
    LogFile logfile;
    std::ostringstream oss;
    oss << time_buf
        << " [" << LevelToStr(lv) << "] "
        << file << ":" << line
        << " | " << content;
    logfile.write(oss.str().c_str());
}

#define LOG_DEBUG(...) PrintLog(LogLevel::DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  PrintLog(LogLevel::INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  PrintLog(LogLevel::WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) PrintLog(LogLevel::ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_FATAL(...) PrintLog(LogLevel::FATAL, __FILE__, __LINE__, __VA_ARGS__)

#endif
#ifndef CLOG_LOGFILE_H
#define CLOG_LOGFILE_H

/**
 * @file logfile.h
 * @brief 日志文件管理器：按“天 + 序号”滚动、按大小切分。
 *
 * 线程模型：
 *   本类 **不是线程安全的**。它只应由 AsyncLogger 的后台写线程串行调用；
 *   不要在业务线程里直接调用 write()/flush()。
 *
 * 文件命名：
 *   <dir><basename>_<YYYYMMDD>_<page>.txt
 *   例如：log/log_20261111_0.txt、log/log_20261111_1.txt
 *
 * 滚动规则：
 *   1. 跨天：打开新一天的文件，序号从 0 重新开始；
 *   2. 单文件超过 m_maxFileSize（默认 100MB）：序号 +1，换新文件；
 *   3. 尽量在换行处切分，避免把一条日志劈成两半。
 */

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <direct.h>  // _mkdir
#endif

class logfile {
public:
    // 禁止拷贝：内部持有 FILE*，拷贝会导致重复关闭/双写。
    logfile(const logfile&) = delete;
    logfile& operator=(const logfile&) = delete;

    /**
     * @brief 进程级单例。
     *
     * 注意：单例只会在“第一次调用”时用传入的参数初始化；
     * 之后所有调用都返回同一个对象，参数被忽略。
     * 因此应尽早、用最终配置调用一次（例如在 main 开头）。
     */
    static logfile& Instance(const std::string& dir,
                             const std::string& basename) {
        static logfile inst(dir, basename);
        return inst;
    }

    /**
     * @brief 构造：创建目录并打开当天第一个文件。
     * @param dir      存放目录，允许相对/绝对路径，允许带尾部 '/'。
     * @param basename 文件名前缀。
     */
    logfile(const std::string& dir, const std::string& basename)
        : m_curFd(nullptr),
          m_curSize(0),
          m_page(0),
          m_baseName(basename),
          m_dir(dir),
          m_curTimeTag(TodayTag()) {
        MkdirP(m_dir);
        OpenCurrentFile();
    }

    ~logfile() { CloseCurrentFile(); }

    /**
     * @brief 追加写入一段文本。
     *
     * 内部会自动处理跨天滚动与按大小切分；可以一次传入多条日志
     * （整批拼接后的字符串），以减少系统调用次数。
     *
     * @param s 待写入内容（可含换行）。
     * @return false 表示文件句柄不可用，数据被丢弃。
     */
    bool write(const std::string& s) {
        if (s.empty()) return true;
        if (!EnsureOpen()) return false;

        // rest 是“尚未写出去的部分”，循环处理跨文件切分。
        std::string rest = s;
        while (!rest.empty()) {
            // —— 跨天检查：每天第一次写入前切换到新文件 ——
            const std::string today = TodayTag();
            if (today != m_curTimeTag) {
                RollByDay();
                if (!EnsureOpen()) return false;
            }

            // 当前文件已满（含“上一行超长把文件撑爆”的情况）：
            // 先滚动到新文件，再重新计算剩余空间。
            if (m_curSize >= m_maxFileSize) {
                RollBySize();
                if (!EnsureOpen()) return false;
                continue;
            }

            const std::uint64_t room = m_maxFileSize - m_curSize;

            // 情况 A：剩余空间足够，一次性写完，收工。
            if (rest.size() <= room) {
                WriteRaw(rest);
                return true;
            }

            // 情况 B：放不下，尝试在换行处切，避免截断一行。
            std::string head = rest.substr(0, static_cast<std::size_t>(room));
            const std::size_t nl = head.rfind('\n');
            if (nl != std::string::npos) {
                // 写完整行，然后滚动到新文件，继续处理剩余部分。
                const std::string part = rest.substr(0, nl + 1);
                WriteRaw(part);
                rest.erase(0, nl + 1);
                RollBySize();
                if (!EnsureOpen()) return false;
                continue;
            }

            // 情况 C：当前文件剩余空间里一个换行都没有。
            if (m_curSize == 0) {
                // 极端情况：单行长度 >= 单文件上限。
                // 硬写进去，否则会死循环；该文件会略超上限。
                WriteRaw(rest);
                return true;
            }

            // 正常情况：直接换新文件，剩余内容写进新文件。
            RollBySize();
            if (!EnsureOpen()) return false;
        }
        return true;
    }

    /**
     * @brief 把用户态缓冲刷到内核。
     * @return true 表示成功；没有打开文件时返回 false。
     */
    bool flush() {
        if (m_curFd == nullptr) return false;
        return std::fflush(m_curFd) == 0;
    }

    // 当前文件完整路径（调试/排障用）。
    std::string CurrentPath() const { return BuildPath(); }

private:
    // ------------------------------------------------------------------
    // 文件生命周期
    // ------------------------------------------------------------------

    // 关闭当前文件并复位大小计数。
    void CloseCurrentFile() {
        if (m_curFd != nullptr) {
            std::fflush(m_curFd);
            std::fclose(m_curFd);
            m_curFd = nullptr;
        }
        m_curSize = 0;
    }

    // 打开“当前时间标签 + 当前序号”对应的文件（追加模式）。
    void OpenCurrentFile() {
        const std::string path = BuildPath();
        m_curFd = std::fopen(path.c_str(), "a");
        if (m_curFd == nullptr) {
            std::cerr << "[Log Error] open failed: " << path
                      << " (" << std::strerror(errno) << ")\n";
            m_curSize = 0;
            return;
        }
        // 以追加模式打开时，文件可能已有内容。
        // 用 ftell 得到已有字节数，保证按大小切分的阈值准确。
        const long sz = std::ftell(m_curFd);
        m_curSize = (sz > 0) ? static_cast<std::uint64_t>(sz) : 0;
    }

    // 确保句柄可用；不可用则尝试重新打开。
    bool EnsureOpen() {
        if (m_curFd != nullptr) return true;
        OpenCurrentFile();
        return m_curFd != nullptr;
    }

    // 跨天滚动：换天、序号归零、打开新文件。
    void RollByDay() {
        CloseCurrentFile();
        m_curTimeTag = TodayTag();
        m_page = 0;
        OpenCurrentFile();
    }

    // 按大小滚动：序号 +1，打开新文件。
    void RollBySize() {
        CloseCurrentFile();
        ++m_page;
        OpenCurrentFile();
    }

    // 实际写入；用 fwrite 而不是 fputs，避免正文里出现 '\0' 时被截断。
    void WriteRaw(const std::string& s) {
        if (s.empty() || m_curFd == nullptr) return;
        const std::size_t n = std::fwrite(s.data(), 1, s.size(), m_curFd);
        m_curSize += static_cast<std::uint64_t>(n);
    }

    // 拼出当前文件完整路径。
    std::string BuildPath() const {
        return m_dir + m_baseName + "_" + m_curTimeTag + "_" +
               std::to_string(m_page) + ".txt";
    }

    // ------------------------------------------------------------------
    // 工具函数
    // ------------------------------------------------------------------

    // 线程安全地取“今天”的标签，格式 YYYYMMDD。
    static std::string TodayTag() {
        const std::time_t now = std::time(nullptr);
        std::tm tm_now{};
#if defined(_WIN32)
        localtime_s(&tm_now, &now);
#else
        localtime_r(&now, &tm_now);
#endif
        char buf[16] = {0};
        std::strftime(buf, sizeof(buf), "%Y%m%d", &tm_now);
        return std::string(buf);
    }

    // 递归创建目录（等价于 mkdir -p），忽略“已存在”错误。
    static void MkdirP(const std::string& dir) {
        if (dir.empty()) return;
        std::string path;
        for (std::size_t i = 0; i < dir.size(); ++i) {
            path.push_back(dir[i]);
#if defined(_WIN32)
            const bool is_sep = (dir[i] == '/' || dir[i] == '\\');
#else
            const bool is_sep = (dir[i] == '/');
#endif
            const bool is_last = (i + 1 == dir.size());
            if (!is_sep && !is_last) continue;

            std::string sub = path;
            while (sub.size() > 1 &&
#if defined(_WIN32)
                   (sub.back() == '/' || sub.back() == '\\')
#else
                   sub.back() == '/'
#endif
            ) {
                sub.pop_back();
            }
            if (sub.empty() || sub == "." || sub == "..") continue;
#if defined(_WIN32)
            _mkdir(sub.c_str());  // 已存在时返回 EEXIST，忽略
#else
            ::mkdir(sub.c_str(), 0755);  // 已存在时返回 EEXIST，忽略
#endif
        }
    }

private:
    FILE*       m_curFd;       // 当前正在写入的文件句柄
    std::uint64_t m_curSize;   // 当前文件已写字节数
    std::uint64_t m_page;      // 当天的文件序号（按大小滚动）
    std::string m_baseName;    // 文件名前缀
    std::string m_dir;         // 存放目录
    std::string m_curTimeTag;  // 当前文件绑定的日期标签 YYYYMMDD

    // 单文件大小上限：100MB。超过后自动换文件。
    static constexpr std::uint64_t m_maxFileSize = 100ULL * 1024 * 1024;
};

#endif  // CLOG_LOGFILE_H

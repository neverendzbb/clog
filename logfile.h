#ifndef logfile_LOG_H
#define logfile_LOG_H
#include <cstdio>
#include <cstdint>
#include <iostream>
#include <sys/stat.h>
#include <string>
class logfile{
private:
    FILE* m_curFd=nullptr;          // 仅当前正在写入的日志文件
    uint64_t m_curSize=0;     // 当前文件已写字节大小
    std::string m_curTimeTag="20260703";    // 当前文件绑定的时间戳（按天切割用）
    uint64_t m_page=0;            //当天日志分的文件个数（按大小切割）
    std::string m_baseName="log";      // 日志基础名 app.log
    std::string dir="log/";           //  存放文件夹路径"log/"相对路径
    // 配置阈值
    uint64_t m_maxFileSize=100ULL * 1024 * 1024; //单文件最大容量100MB
    uint64_t m_maxBackup;                            //存储最多文件个数
    std::string combinpath(){
        std::string truepath=dir+m_baseName+"_"+m_curTimeTag+"_"
                        +std::to_string(m_page)+".txt";
        return truepath;
    }
    void splitday(){
        closeCurrentFile();
        m_page=0;
        m_curTimeTag=getTodayTag();
        m_curFd=std::fopen(combinpath().c_str(), "a");
        if (!m_curFd)
        {
            std::cerr << "[Log Error] splitday open failed: " << combinpath() << "\n";
        }
    }
    void splitsize(){
        if(m_curTimeTag!=getTodayTag()){
            splitday();
            return;
        }
        closeCurrentFile();
        m_page++;
        m_curTimeTag=getTodayTag();
        m_curFd=std::fopen(combinpath().c_str(), "a");
        if (!m_curFd)
        {
            std::cerr << "[Log Error] splitsize open failed: " << combinpath() << "\n";
        }
    }
// 返回 20260703 格式字符串
std::string getTodayTag()
{
    time_t now = time(nullptr);
    tm* t = localtime(&now);
    char buf[32] = {0};
    strftime(buf, sizeof(buf), "%Y%m%d", t);
    return std::string(buf);
}
void closeCurrentFile()
{
    if(m_curFd != nullptr)
    {
        fflush(m_curFd);
        fclose(m_curFd);
        m_curFd = nullptr;
    }
    m_curSize = 0;
}
public:
    // Non-copyable
    logfile(const logfile&) = delete;
    logfile& operator=(const logfile&) = delete;

    static logfile& Instance(const std::string path,const std::string basename)
    {
        static logfile inst(path,basename);
        return inst;
    }

    logfile(const std::string path,const std::string basename){
        dir=path;
        m_baseName=basename;
        m_curTimeTag=getTodayTag();
        m_page=0;
        mkdir(dir.c_str(), 0755);
        m_curFd=std::fopen(combinpath().c_str(), "a");
        if (!m_curFd)
        {
            std::cerr << "[Log Error] init log file open failed: " << combinpath() << "\n";
        }
        
    }
    ~logfile(){
        closeCurrentFile();
    }
    bool write(std::string s){
        if (!m_curFd)
        {
            std::cerr << "[Log Error] file handle null, write skip\n";
            return false;
        }
        if(getTodayTag()!=m_curTimeTag){
            splitday();
             if (!m_curFd) return false;
        }
        uint64_t sizes=s.size();
        while(m_curSize+sizes>m_maxFileSize){
            std::string s1=s.substr(0,m_maxFileSize-m_curSize);
            size_t lastLinePos = s1.rfind('\n');
            if(lastLinePos!=std::string::npos){
                s1=s.substr(0,lastLinePos+1);
            }
            else{
                splitsize();
                if (!m_curFd){
                    return false;
                }
                continue;
            }
            std::fputs(s1.c_str(), m_curFd);
            m_curSize += s1.size();
            splitsize();
            if (!m_curFd) {
                return false;
            }
            s = s.substr(lastLinePos + 1);
            sizes = s.size();
        }
        std::fputs(s.c_str(), m_curFd);
        m_curSize += s.size();
        return true;
    }

    bool flush()
    {
        if (m_curFd) {
            return std::fflush(m_curFd) == 0;
        }
        return false;
    }
};
#endif
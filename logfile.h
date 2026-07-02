#ifndef LOG_FILE_H
#define LOG_FILE_H
#include <cstdio>
#include <string>

class LogFile
{
private:
    FILE* fp;
public:
    LogFile(const std::string& path = "log.txt")
    {
        fp = fopen(path.c_str(), "a"); // a追加模式
    }

    ~LogFile()
    {
        if (fp) fclose(fp);
    }

    void write(const char* str)
    {
        if (fp)
        {
            fputs(str, fp);
            fflush(fp); // 立刻刷盘
        }
    }
};
#endif
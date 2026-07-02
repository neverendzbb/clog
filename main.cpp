#include "simple_log.h"
int main()
{
    LOG_INFO("服务启动，端口 %d", 8080);
    LOG_ERROR("客户端解析失败 fd=%d", 10);
    return 0;
}
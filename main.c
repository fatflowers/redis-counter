#include "rediscounter.h"

int main(void)
{
    rdbLoad("/home/simon/redis-counter/dump.rdb");
    printf("Hello World!\n");
    return 0;
}


#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(void)
{
    char* line = NULL;
    size_t len = 0;
    int read = getline(&line, &len, stdin);
    while(read != -1)
    {
        printf("1\n");
        printf("Hi, I'll sleep here ");
        sleep(120);
        printf("%s", line);
        read = getline(&line, &len, stdin);
    }
    free(line);
    return 0;
}

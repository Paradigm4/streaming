#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

int normal()
{
    char* line = NULL;
    size_t len = 0;
    int read = getline(&line, &len, stdin);
    while(read > 0)
    {
        char * end = line;
        errno = 0;
        int nLines = strtoll(line, &end, 10);
        printf("%i\n",nLines+1);
        if(errno!=0  || nLines == 0 || (*end) != '\n')
        {
            return 1;
        }
        size_t i;
        for( i =0 ; i<nLines; ++i)
        {
            read = getline(&line, &len, stdin);
            if(read <= 0)
            {
                return 1;
            }
            printf("Hello\t%s", line);
        }
        printf("OK\tthanks!\n");
        fflush(stdout);
        read = getline(&line, &len, stdin);
    }
    free(line);
    return 0;
}

int main(void)
{
    return normal();
}

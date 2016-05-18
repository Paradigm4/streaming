#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <sstream>
#include <iostream>

using std::string;
using std::ostringstream;
using std::cout;

int normal(unsigned int read_delay = 0, unsigned int write_delay = 0)
{
    char* line = NULL;
    size_t len = 0;
    int read = getline(&line, &len, stdin);
    while(read > 0)
    {
        char * end = line;
        errno = 0;
        int nLines = strtoll(line, &end, 10);
        if(errno!=0  || nLines == 0 || (*end) != '\n')
        {
            return 1;
        }
        ostringstream output;
        output<<nLines+1<<"\n";
        size_t i;
        for( i =0 ; i<nLines; ++i)
        {
            read = getline(&line, &len, stdin);
            if(read <= 0)
            {
                return 1;
            }
            sleep(read_delay);
            output<<"Hello\t"<<line;
        }
        output<<"OK\tthanks!\n";
        string outputString = output.str();
        string firstPacket = outputString.substr(0,1024);
        string secondPacket = outputString.substr(1024,outputString.length());
        cout<<firstPacket;
        cout<<std::flush;
        sleep(write_delay);
        cout<<secondPacket;
        cout<<std::flush;
        read = getline(&line, &len, stdin);
    }
    free(line);
    return 0;
}


int main(void)
{
    return normal(0,00);
}

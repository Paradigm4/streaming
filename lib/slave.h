/* forking library functions */
#include <sys/time.h>
#include <sys/resource.h>

typedef struct slaves {
  pid_t pid;  // slave process id
  int in;     // slave process stdin descriptor, parent writes to this
  int out;    // slave process stdout descriptor, parent reads from this
} slave;

typedef struct limits {
  rlim_t DATA;     // Max program memory space in bytes
  rlim_t STACK;    // Max program stack space in bytes
  rlim_t CPU;      // Max CPU time in seconds
  rlim_t NOFILE;   // Max number of open files + 1 (forced to be at least 8)
} limits;

// start a slave process
// @param argv, envp the process command line and optional environment
// @param lim optional process ulimits, set to NULL to skip setting limits, otherwise use the limits structure above
//        (NULL terminated string)
// @return a slave value; if an error occurred then slave.pid = -1.
slave run (char* const argv[], char* const envp[], limits* lim);

/* forking library functions */

typedef struct slaves {
  pid_t pid;  // slave process id
  int in;     // slave process stdin descriptor, parent writes to this
  int out;    // slave process stdout descriptor, parent reads from this
} slave;

// start a slave process
// @param argv, envp the process command line and optional environment
//        (NULL terminated string)
slave run(char **argv, char **envp);

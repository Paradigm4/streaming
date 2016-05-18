#define _GNU_SOURCE
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "slave.h"

slave
run (char* const argv[], char* const envp[], limits* lim)
{
  int j, status;
  slave s;
  int parent_child[2];          // pipe descriptors parent writes to child
  int child_parent[2];          // pipe descriptors child writes to parent
  pipe (parent_child);
  pipe (child_parent);
  switch (s.pid = fork ())
    {
    case -1:
      s.pid = -1;
      break;
    case 0:                    // child
      close (1);
      dup (child_parent[1]);    // stdout writes to parent
      close (0);
      dup (parent_child[0]);    // parent writes to stdin
      close (parent_child[1]);
      close (child_parent[0]);
      execvpe (argv[0], argv, envp);
      abort(); //if execvpe returns, it means we're in trouble. bail asap.
      break;
    default:                   // parent
      if (lim != NULL)
      {
          // first validate the limits
          if (lim->NOFILE < 8)
            lim->NOFILE = 8;
          // now try to set them
          j = 0;
          struct rlimit rl;
          rl.rlim_cur = lim->DATA;
          rl.rlim_max = lim->DATA;
          j = j || (prlimit (s.pid, RLIMIT_DATA, &rl, NULL) < 0);
          rl.rlim_cur = lim->STACK;
          rl.rlim_max = lim->STACK;
          j = j || (prlimit (s.pid, RLIMIT_STACK, &rl, NULL) < 0);
          rl.rlim_cur = lim->CPU;
          rl.rlim_max = lim->CPU;
          j = j || (prlimit (s.pid, RLIMIT_CPU, &rl, NULL) < 0);
          rl.rlim_cur = lim->NOFILE;
          rl.rlim_max = lim->NOFILE;
          j = j || (prlimit (s.pid, RLIMIT_NOFILE, &rl, NULL) < 0);
          if (j)               // something went wrong setting the limits!
          {
              kill (s.pid, SIGKILL);
              waitpid (s.pid, &status, 0);
              s.pid = -1;
              return s;
          }
      }
      close (parent_child[0]);
      close (child_parent[1]);
      s.in = parent_child[1];
      s.out = child_parent[0];
    }
    return s;
}

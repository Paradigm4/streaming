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
run (char **argv, char **envp, limits * lim)
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
    default:                   // parent
      if (lim != NULL)
        {
          // first validate the limits
          if (lim->NPROC < 1)
            lim->NPROC = 1;
          if (lim->NOFILE < 5)
            lim->NOFILE = 5;
          // now try to set them
          j = 1;
          struct rlimit rl;
          rl.rlim_cur = lim->AS;
          rl.rlim_max = lim->AS;
          j = j && (prlimit (s.pid, RLIMIT_AS, &rl, NULL) < 0);
          rl.rlim_cur = lim->CPU;
          rl.rlim_max = lim->CPU;
          j = j && (prlimit (s.pid, RLIMIT_CPU, &rl, NULL) < 0);
          rl.rlim_cur = lim->NPROC;
          rl.rlim_max = lim->NPROC;
          j = j && (prlimit (s.pid, RLIMIT_NPROC, &rl, NULL) < 0);
          rl.rlim_cur = lim->NOFILE;
          rl.rlim_max = lim->NOFILE;
          j = j && (prlimit (s.pid, RLIMIT_NOFILE, &rl, NULL) < 0);
          if (!j)               // something went wrong setting the limits!
            {
              kill (s.pid, SIGKILL);
              waitpid (s.pid, &status, WNOHANG);
            }
        }
      close (parent_child[0]);
      close (child_parent[1]);
      s.in = parent_child[1];
      s.out = child_parent[0];
    }
  return s;
}

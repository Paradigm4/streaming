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
run (char const *command, char *const envp[], limits * lim)
{
  int j;
  unsigned long i;
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
      struct rlimit limit;
      getrlimit (RLIMIT_NOFILE, &limit);
      for (i = 3; i < limit.rlim_max; i = i + 1)
        {
          close (i);
        }

      if (lim != NULL)
        {
          // first validate the limits
          if (lim->NOFILE < 8)
            lim->NOFILE = 8;
          // now try to set them
          j = 0;
          limit.rlim_cur = lim->DATA;
          limit.rlim_max = lim->DATA;
          j = j || (setrlimit (RLIMIT_DATA, &limit) < 0);
          limit.rlim_cur = lim->STACK;
          limit.rlim_max = lim->STACK;
          j = j || (setrlimit (RLIMIT_STACK, &limit) < 0);
          limit.rlim_cur = lim->CPU;
          limit.rlim_max = lim->CPU;
          j = j || (setrlimit (RLIMIT_CPU, &limit) < 0);
          limit.rlim_cur = lim->NOFILE;
          limit.rlim_max = lim->NOFILE;
          j = j || (setrlimit (RLIMIT_NOFILE, &limit) < 0);
          if (j)                // something went wrong setting the limits!
            {
              abort ();
            }
        }


      execle ("/bin/bash", "/bin/bash", "-c", command, NULL, envp);
      abort ();                 //if execvpe returns, it means we're in trouble. bail asap.
      break;
    default:                   // parent
      close (parent_child[0]);
      close (child_parent[1]);
      s.in = parent_child[1];
      s.out = child_parent[0];
    }
  return s;
}

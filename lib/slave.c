#define _GNU_SOURCE
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>

#include "slave.h"

slave
run (char **argv, char **envp)
{
  slave s;
  int parent_child[2];          // pipe descriptors parent writes to child
  int child_parent[2];          // pipe descriptors child writes to parent

  pipe (parent_child);
  pipe (child_parent);
  switch (s.pid = fork ())
    {
    case -1:
      exit (1);
    case 0:                    // child
      close (1);
      dup (child_parent[1]);    // stdout writes to parent
      close (0);
      dup (parent_child[0]);    // parent writes to stdin
      close (parent_child[1]);
      close (child_parent[0]);
      execvpe (argv[0], argv, envp);
    default:                   // parent
      close (parent_child[0]);
      close (child_parent[1]);
      s.in = parent_child[1];
      s.out = child_parent[0];
      break;
    }
  return s;
}

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>

#include "serial.h"
#include "slave.h"

void
FREE (char **x)
{
  if (*x)
    {
      free (*x);
      *x = NULL;
    }
}

int
main (int argc, char **argv)
{
  char buf;
  char *line = NULL;
  FILE *fp;
  size_t len;
  int j, n;
  // slave limits, try chaning these!
  limits lim;
  lim.DATA = 1000000000;        // memory limit (bytes)
  lim.STACK = 1000000;          // stack limit (bytes)
  lim.CPU = 100;                // CPU limit (seconds)
  lim.NOFILE = 8;               // max total open files

  // the program command line and limits
  slave s = run (argv + 1, NULL, &lim);
  fp = fdopen (s.out, "r");     // need for getline below
  if (fp == NULL)
    goto end;
  // invocation without limits
//  slave s = run (argv + 1, NULL, NULL);

  // IMPORTANT: check return value
  if (s.pid < 0)
    {
      fprintf (stderr, "fork failed, bummer\n");
      return -1;
    }
  fprintf (stderr, "slave pid is %d\n", (int) s.pid);

  // program data (row-wise TSV)
  char *chunk_1 = "a\tb\tc\n4\t5\t6\n";
  char *chunk_2 = "1\t2\t3\nd\te\tf\n";

// Write chunk_1 data to slave
  if (write_tsv (s.in, chunk_1, 2) < 0)
    goto end;

// Read character output from slave and print it out (TSV)
  len = 0;
  if (getline (&line, &len, fp) < 0)
    goto end;
  n = atoi (line);
  for (j = 0; j < n; ++j)
    {
      if (getline (&line, &len, fp) < 0)
        goto end;
      printf ("output line=%s", line);
    }
  FREE (&line);

// Write chunk_2 data to slave
  if (write_tsv (s.in, chunk_2, 2) < 0)
    goto end;
// Read the results
  if (getline (&line, &len, fp) < 0)
    goto end;
  n = atoi (line);
  for (j = 0; j < n; ++j)
    {
      if (getline (&line, &len, fp) < 0)
        goto end;
      printf ("output line=%s", line);
    }
  FREE (&line);

end:
  FREE (&line);
  close (s.in);
  close (s.out);
// Send SIGTERM TO slave and wait to remove zombies (example of cleanly killing process)
  kill (s.pid, SIGTERM);
  int status;
  waitpid (s.pid, &status, WNOHANG);
  if (!WIFEXITED (status))
    {
      kill (s.pid, SIGKILL);
      waitpid (s.pid, &status, WNOHANG);
    }
  return status;
}

/* self-contained test program:
 * send 5 integers and 5 doubles to a process stdin in R's columnar binary formatted as list(x=as.integer(1:5), y=as.double(1:5))
 * then just read the character output of the process stdout and print it
 * also show how the limits work
 */
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>

#include "serial.h"
#include "slave.h"

int
main (int argc, char **argv)
{
  // program limits
  limits lim;
  lim.AS = 10000000000;          // memory limit (bytes)
  lim.CPU = 1000;                // CPU limit (seconds)
  lim.NPROC = 2;                // max num. threads
  lim.NOFILE = 500;             // max total open files

  // the program command line and limits
//  slave s = run (argv + 1, NULL, &lim);
  // invocation without limits
  slave s = run (argv + 1, NULL, NULL);

  // IMPORTANT: check return value
  if (s.pid < 0)
    {
      fprintf (stderr, "fork failed, bummer\n");
      return -1;
    }

  // program data
  int buf[] = { 1, 2, 3, 4, 5 };
  double db[] = { 1, 2, 3, 4, 5 };
  char *name[] = { "x", "y" };
  int n[] = { 1, 1 };
  fprintf (stderr, "slave pid is %d\n", (int) s.pid);

// Write data to slave
// XXX check these for error (-1) return value
  write_header (s.in, 2);
  write_ints (s.in, buf, 5);
  write_doubles (s.in, db, 5);
  write_names (s.in, name, n, 2);
  close (s.in);

// Read character output from slave and print it out
  while (read (s.out, &buf, 1) > 0)     // slave stdout -> stdout
    write (1, &buf, 1);
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

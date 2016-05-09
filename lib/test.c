/* self-contained test program: send 5 integers, 5 doubles and 5 strings to a
 * process stdin in R's columnar binary formatted as list(x=as.integer(1:5),
 * y=as.double(1:5)) then just read the character output of the process stdout
 * and print it also show how the limits work
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
  char buf;
  // slave limits, try chaning these!
  limits lim;
  lim.DATA = 1000000000;      // memory limit (bytes)
  lim.STACK = 1000000;        // stack limit (bytes)
  lim.CPU = 100;              // CPU limit (seconds)
  lim.NOFILE = 8;             // max total open files

  // the program command line and limits
  slave s = run (argv + 1, NULL, &lim);
  // invocation without limits
//  slave s = run (argv + 1, NULL, NULL);

  // IMPORTANT: check return value
  if (s.pid < 0)
    {
      fprintf (stderr, "fork failed, bummer\n");
      return -1;
    }

  // program data
  int integer_attribute[] = { 1, 2, 3, 4, 5 };
  double double_attribute[] = { 1, 2, 3, 4, 5 };
  char * string_attribute[] = { "string 1", "string 2", "string 3", "string 4", "string 5" };
  int sn[] = { 8, 8, 8, 8, 8 };
  char *name[] = { "x", "y", "z" };
  int n[] = { 1, 1, 1 }; // string length of each name
  fprintf (stderr, "slave pid is %d\n", (int) s.pid);

// Write data to slave
// XXX check these for error (-1) return value
  write_header (s.in, 3);
  write_ints (s.in, integer_attribute, 5);
  write_doubles (s.in, double_attribute, 5);
  write_strings (s.in, string_attribute, sn, 5);
  write_names (s.in, name, n, 3);
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

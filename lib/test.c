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
  slave s = run (argv + 1, NULL);
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

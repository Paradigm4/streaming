/* mini R serialization library
 * only supports a subset of R binary serialization
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>

/* The R_HEADER starts with B\n indicating native binary format */
const char R_HEADER[] =
  { 0x42, 0x0a, 0x02, 0x00, 0x00, 0x00, 0x02, 0x02, 0x03, 0x00, 0x00, 0x03,
  0x02, 0x00
};
const char R_VECSXP[] = { 0x13, 0x02, 0x00, 0x00 };     // R list with attributes
const char R_INTSXP[] = { 0x0d, 0x00, 0x00, 0x00 };
const char R_REALSXP[] = { 0x0e, 0x00, 0x00, 0x00 };
const char R_CHARSXP[] = { 0x09, 0x00, 0x04, 0x00 };    // UTF-8
const char R_STRSXP[] = { 0x10, 0x00, 0x00, 0x00 };
const char R_LISTSXP[] = { 0x02, 0x04, 0x00, 0x00 };    // internal R pairlist

ssize_t
write_tsv (int fd, char const* buf, int nlines)
{
  char hdr[4096];
  snprintf (hdr, 4096, "%d\n", nlines);
  size_t n = strlen (hdr);
  if (write (fd, hdr, n) < n)
    return -1;
  return write (fd, buf, strlen (buf));
}


/* first step
 * fd     the file descriptor to write to
 * length the number of variables (list length)
 */
ssize_t
write_header (int fd, int length)
{
  char buf[4096];
  char *p = buf;
  size_t n;
  memcpy (p, R_HEADER, sizeof (R_HEADER));
  p += sizeof (R_HEADER);
  memcpy (p, R_VECSXP, sizeof (R_VECSXP));
  p += sizeof (R_VECSXP);
  n = p - buf;
  if (write (fd, buf, n) < n)
    return -1;
  return write (fd, &length, 4);
}

// Last step
ssize_t
write_names (int fd, char **buffer, int *n, int length)
{
  char HDR[] =
    { 0x02, 0x04, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x09, 0x00, 0x04, 0x00,
    0x05, 0x00, 0x00, 0x00, 0x6e, 0x61, 0x6d, 0x65, 0x73
  };
  char NEND[] = { 0xfe, 0x00, 0x00, 0x00 };
  int j;
  if (write (fd, HDR, sizeof (HDR)) < sizeof (HDR))
    return -1;
  if (write (fd, R_STRSXP, sizeof (R_STRSXP)) < sizeof (R_STRSXP))
    return -1;
  if (write (fd, &length, sizeof (length)) < sizeof (length))
    return -1;
  for (j = 0; j < length; ++j)
    {
      if (write (fd, R_CHARSXP, sizeof (R_CHARSXP)) < sizeof (R_CHARSXP))
        return -1;
      if (write (fd, &(n[j]), 4) < 4)
        return -1;
      if (write (fd, buffer[j], n[j]) < n[j])
        return -1;
    }
  return write (fd, NEND, 4);
}

// write length doubles from a buffer to the file descriptor
ssize_t
write_doubles (int fd, double *buffer, int length)
{
  if (write (fd, R_REALSXP, sizeof (R_REALSXP)) < sizeof (R_REALSXP))
    return -1;
  if (write (fd, &length, sizeof (int)) < sizeof (int))
    return -1;
  return write (fd, buffer, length * sizeof (double));
}

// write length strings, each of length n[j], from a buffer to the file desc
ssize_t
write_strings (int fd, char **buffer, int *n, int length)
{
  int j;
  if (write (fd, R_STRSXP, sizeof (R_STRSXP)) < sizeof (R_STRSXP))
    return -1;
  if (write (fd, &length, sizeof (length)) < sizeof (length))
    return -1;
  for (j = 0; j < length; ++j)
    {
      if (write (fd, R_CHARSXP, sizeof (R_CHARSXP)) < sizeof (R_CHARSXP))
        return -1;
      if (write (fd, &(n[j]), 4) < 4)
        return -1;
      if (write (fd, buffer[j], n[j]) < n[j])
        return -1;
    }
  return j;
}

// write length ints from a buffer to the file descriptor
ssize_t
write_ints (int fd, int *buffer, int length)
{
  if (write (fd, R_INTSXP, sizeof (R_INTSXP)) < sizeof (R_INTSXP))
    return -1;
  if (write (fd, &length, sizeof (int)) < sizeof (int))
    return -1;
  return write (fd, buffer, length * sizeof (int));
}

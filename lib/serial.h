/* Micro R serialization library
 * Only supports a tiny subset of R binary serialization
 * in native encoding (not XDR)
 *
 * Usage:
 * write_header length = number of attributes
 * write_xxx    first attribute
 * write_xxx    2nd attribute
 * ...          etc.
 * write_names  write the names of the attributes
 * done
 *
 * see test.c and Make test for an example
 */

// @param fd file descriptor to write to
// @return a ssize_t value, negative value means error, otherwise OK

// @param length number of attributes
ssize_t
write_header (int fd, int length);
// @param buffer of doubles
// @param length of buffer in number of doubles
ssize_t
write_doubles (int fd, double *buffer, int length);
// @param buffer of ints
// @param length of buffer in number of ints
ssize_t
write_ints (int fd, int *buffer, int length);
// @parm buffer of character arrays
// @param n array of integer lengths of each string in the buffer, not including trailing NULL byte
// @param length number of strings (length of buffer)
ssize_t
write_strings (int fd, char **buffer, int *n, int length);
// @parm buffer of character arrays
// @param n array of integer lengths of each string in the buffer, not including trailing NULL byte
// @param length number of names, must match length in write_header function
ssize_t
write_names (int fd, char **buffer, int *n, int length);

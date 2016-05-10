# streaming
Prototype Hadoop streaming-like SciDB API

The operator sends SciDB array data into the stdin of the process and reads its
stdout (hence 'streaming').

This needs an operator something like:
```
stream(array, command, args, format, ...)
```
- `...` are maybe some process ulimit settings,
- `command` program to run
- `args` command-line arguments
- `format` save format

I envision only two save formats: columnar binary form eventually feather, but
now native R form, and row-wise TSV. I think for simplicity we should assume
symmetry of format on input and output.

## notes

Perhaps the best approach might be for each instance to fork a slave process,
and then stream one chunk at a time through it and consume the output. This
requires a process that coorperates with that approach (pretty easy for
row-wise TSV I/O, less so maybe for columnar I/O).

What do you think?

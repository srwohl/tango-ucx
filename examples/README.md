# Examples

Examples are a separate CMake consumer project. They use only the installed
`tango-ucx` package and are excluded from the library and test line budget.

- `archive`: an archiver for an opaque UCX stream. It joins a device as an every
  subscription and writes each batch as one chunk: a header, each frame's offset and
  length, the frames' records, then the frames as one byte blob, written from the receive
  ring with one `writev`. `archive.h` is the chunk format, with a reader;
  `tests/transport/test_archive.cpp` checks it and holds the write benchmark.

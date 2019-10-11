\d .ap

// Append bytes to the global buffer
/* bytes  = bytes to be appended
/. return = Number of bytes written
i.bufferAppend:{[bytes]
  $[not i.bufferWrittenTo;
    [(buffer`:seek) io[`:SEEK_SET]`;
      i.bufferWrittenTo:1b];
    (buffer`:seek) io[`:SEEK_END]`];
  buffer[`:write][bytes]`
  }


// Read the contents of a buffer
/* amount  = number of bytes to be read from the start of the buffer (::) returns full buffer
/. returns = the content that has been read
i.bufferRead:{[amount]
  (buffer`:seek) io[`:SEEK_SET]`;
  (buffer`:read)[amount]
  }


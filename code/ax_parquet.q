\d .pq

// Required python packages
pq:.p.import[`pyarrow.parquet]
io:.p.import`io

// initialize the buffer
buffer:io[`:BytesIO][]

// Boolean flag to track if the current buffer has been written to
// This is needed because the buffer append function always writes starting at the end of the buffer
// but a newly initialized buffer isn't necessarily of size 0, so special behaviour is needed for new buffers
i.bufferWrittenTo:0b

// Write an arrow table to a parquet table on disk
/* arrowTable = foreign
/* target     = path or file object table is written to {symbol|string|hsym|foreign}
/* opts       = dictionary of options that can be passed to pykwargs from pq[`:write_table] (link to docs)
/. return     = null
arrowTableToFile:{[arrowTable;target;opts]
  if[-11h~type target;target:`$$[":"~first s:string target;1_s;s]];
  opts:$[opts~(::);()!();opts];
  pq[`:write_table][arrowTable;target;pykwargs opts];
  }


// Write an arrow table to a partitioned Parquet dataset
/* arrowTable = foreign
/* target     = path or file object table is written to {symbol|string|hsym|foreign}
/* opts       = dictionary of options that can be passed to pykwargs from pq[`:write_to_dataset] (link to docs)
/. return     = null
arrowTableToPart:{[arrowTable;path;opts]
  if[-11h~type path;path:`$$[":"~first s:string path;1_s;s]];
  opts:$[opts~(::);()!();opts];
  pq[`:write_to_dataset][arrowTable;`$$[":"~first s:string path;1_s;s];pykwargs opts];
  }


// This can be called once a buffer has been filled with a Parquet table to get an arrow table
/* buffer = a buffer full with a Parquet table
/. return = the associated arrow table
bufferToArrowTable:{[buffer]
  pq[`:read_table] buffer
  }


// Output an Arrow table based on a provided path an subset of columns
/* target = path or file object table is written to as a symbol, string, hsym or foreign
/. return = an arrow table
fileToArrowTable:{[target;columns]
  if[-11h~type target;target:`$$[":"~first s:string target;1_s;s]];
  .p.wrap pq[`:read_table][>;target;pykw[`columns;columns]]
  }


// Get the metadata of a parquet file
/* path    = the path as a symbol to the parquet file/directory
/. returns = a dictionary containing the appropriate information 
metadata:{[path]
  .aw.metadata fileToArrowTable[path;::]
  }

// Import a parquet table from disk
/* path    = the path as a symbol to the parquet file/directory
/* opts    = options which can be modified include 
/            opts`columns    -> columns to load
/            opts`batch      -> specifify which row group should be imported
/            opts`transforms -> mapping of column names to functions to be to the column
/            opts`casts      -> mapping of column names to type columns should be cast to
/. returns = table
readTable:{[path;opts]
 opts:$[(opts~(::)) or opts~()!();
   (enlist`)!enlist(::);
   (enlist[`]!enlist(::))^opts];
   .aw.readTable[fileToArrowTable[path;opts`columns];
   opts`batch;
   opts`transforms;
   opts`casts
   ]
 }

// Replace the global buffer with a new one 
/. returns = The buffer object as a foreign
replaceBuffer:{[]
    buffer_written_to:1b;
    io`:BytesIO[]
    }


// Write a kdb+ table to a Parquet file on disk
/* t      = kdb+ table to be written to disk
/* target = path or file object to write the table to
/* opts   = dictionary of options that can be passed to pq[`:write_table] (will provide link in the docs)
writeTable:{[t;target;opts]
  arrowTableToFile[;target;opts] .aw.toArrowTable t
  }



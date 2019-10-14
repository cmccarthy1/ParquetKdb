// This file contains all of the callable functionality now present within the ParquetKdb interface,
//   in most cases documentation on code.kx.com will replace the variable definitions and function 
//   definitions presented here.


\d .pq

// Writing kdb+/arrow tables to disk

// Write a kdb+ table or arrow table to feather or parquet format on disk
/* typ     = `feather/`parquet (extensible for addition of avro in future)
/* t       = arrow or kdb+ table to write to disk
/* target  = the path to the file being written as sym, hsym or foreign
/* opts    = dict of options that can be passed to pq[`:write_table] for parquet, unused for feather default = (::)
/. returns = does not return an output to console on correct execution, will write file to disk
writeTable:{[typ;t;target;opts]
  $[typ =`feather;
      feather[`:write_feather][;parsePath target]$[98h~type t;tableToDataframe;]t;
    typ=`parquet;
      arrowToFile[;target;opts]$[98h~type t;kdbToArrow;]t
    '`$"first argument must be either `feather or `parquet"];
  }



// Interrogating various table formats to return appropriate metadata
// Wrapper function for the collection of metadata information for a number of types of table
/* t = input table, path to the file for parquet on disk as sym or hsym, foreign/wrapped object for arrow table or kdb table
/. returns = appropriate metadata information (dictionary for apache 'meta' for kdb)
metadata:{[t]
  $[type[t]in(105h;112h);
      i.metadata t;
    -11h=type[t];
      i.metadata fileToArrow[t;::];
    98h~type t;
      meta t;
    '`$"incorrect argument type provided see documentation for appropriate inputs"]
  }



// Reading various formats to return q tables

// Convert a arrow table to a q table applying casts prior to transform
/* arrowTable = foreign of the table to be converted
/* batch      = optional argument to specifify which row group should be imported (long/null)
/* transforms = mapping of column names to functions to apply to the column (dict [symbol:fn])
/* casts      = special case of transforms to cast non-strings and tok strings as specified (dict [symbol: symbol])
readArrow:{[arrowTable;batch;transforms;casts]
  t:(transforms~(::))|transforms~()!();
  c:(casts~(::))|casts~()!();
  caster:{$[10h~type first y;upper[x]$y;x$y]};
  $[t;::;![;();0b;k!value[transforms],'k:key transforms]]
    $[c;::;![;();0b;k!caster,'(i.casts value casts),'k:key casts]]
    dataframeToTable i.toPandas $[batch~(::);arrowTable;arrowTable[`:to_batches][][@;batch]]
  }

// Reads a feather file to obtain a kdb+ table
/* target  = the path to the feather file
/* opts    = dictionary indicating columns to read ,`columns!col_list or (::)
/. returns = the table from the feature file
readFeather:{[target;opts]readArrow[;::;::;::] featherToArrow[target;opts]}

// Read a parquet table from disk to a q table
/* path    = the path as a symbol to the parquet file/directory
/* opts    = options which can be modified include
/            opts`columns    -> columns to load
/            opts`batch      -> specifify which row group should be imported
/            opts`transforms -> mapping of column names to functions to be to the column
/            opts`casts      -> mapping of column names to type columns should be cast to
/. returns = a q table
readParquet:{[path;opts]
  opts:$[(opts~(::)) or opts~()!();(enlist`)!enlist(::);(enlist[`]!enlist(::))^opts];
  readArrow[fileToArrow[path;opts`columns];opts`batch;opts`transforms;opts`casts]
  }



// Converting various formats to Arrow tables

// Read an arrow table from disk as an arrow table
/* target  = path or file object table is written to as a symbol, hsym or foreign
/* columns = the columns we are reading from disk as a list or (::) which returns full table
/. return  = an arrow table
fileToArrow:{[target;columns]pq[`:read_table][parsePath target;pykw[`columns;columns]]}

// converts a q table to an arrow table
// t        = a q table
/. return = the arrow table as an EmbedPy object
kdbToArrow:{[t] patab[`:from_pandas] tableToDataframe t}

// Read a feather file to obtain an Arrow table
/* target  = a symbol,string,hsym or foreign with the path to the feather file
/* opts    = dictionary indicating columns to read ,`columns!col_list or (::)/()!()
/. returns = an Arrow table
featherToArrow:{[target;opts]
  opts:$[(opts~(::)) or opts~()!();(enlist`)!enlist(::);(enlist[`]!enlist(::))^opts];
  feather[`:read_table][parsePath target;opts`columns]
  }



// Writing Arrow table formats to parquet format

// Write an arrow table to a parquet table on disk
/* arrowTable = foreign
/* target     = path or file object table is written to {symbol|string|hsym|foreign}
/* opts       = dictionary of options that can be passed to pykwargs from pq[`:write_table] (link to docs)
/. return     = null
arrowToFile:{[arrowTable;target;opts]pq[`:write_table][arrowTable;parsePath target;pykwargs $[opts~(::);()!();opts]];}

// Write an arrow table to a partitioned Parquet dataset
/* arrowTable = foreign
/* target     = path or file object table is written to {symbol|string|hsym|foreign}
/* opts       = dictionary of options that can be passed to pykwargs from pq[`:write_to_dataset] (link to docs)
/. return     = null
arrowToPart:{[arrowTable;path;opts]pq[`:write_to_dataset][arrowTable;parsePath path;pykwargs $[opts~(::);()!();opts]];}



// Buffer Functionality

// initialize the buffer
buffer:io[`:BytesIO][]

// This can be called once a buffer has been filled with a Parquet table to get an arrow table
/* buffer = a buffer full with a Parquet table
/. return = the associated arrow table
bufferToArrow:{[buffer]pq[`:read_table] buffer}

// Replace the global buffer with a new one
/. returns = The buffer object as a foreign
replaceBuffer:{[]buffer_written_to:1b;io`:BytesIO[]}


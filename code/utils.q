\d .pq

// Parquet specific  utils

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

// Boolean flag to track if the current buffer has been written to
// This is needed because the buffer append function always writes starting at the end of the buffer
// but a newly initialized buffer isn't necessarily of size 0, so special behaviour is needed for new buffers
i.bufferWrittenTo:0b

// Arrow specific utils

i.casts:(!) . flip {raze (raze each x),\:'y} . flip (enlist each a,'upper a),'string first each a:(
  `b`bool`boolean;`g`guid;`x`byte;`h`short;`i`int;
  `j`long;`e`real;`f`float;`c`string;`s`symbol;`p`timestamp;
  `m`month;`d`date;`z`datetime;`n`timespan;`u`minute;
  `v`second;`t`time
   )

p)def pyMetadata(arrowTable):
   return {'num_row_groups': len(arrowTable.to_batches()),
   'num_rows': arrowTable.num_rows,
   'columns': arrowTable.schema.names,
   'types': [str(x) for x in arrowTable.schema.types]
   }

// Functions for the extraction of metadata from an arrow table
/* arrowTable = arrow table as foreign/wrapped embedPy object
/. returns    = metadata as a q dictionary
i.metadata:{[arrowTable]@[m;c;:;`$(m:.p.get[`pyMetadata;<]arrowTable)c:`columns`types]}

// convert an arrow table to a Pandas DataFrame
/* arrowTable = the arrow table to be converted
/. returns    = the Pandas DataFrame as a foreign object
i.toPandas:{[arrowTable]
  arrowTable[`:to_pandas]pykw[`zero_copy_only;i.zeroCopyOnly]
  }


// enforce a conversion to Pandas DataFrame (only when a copy is not being made)
i.zeroCopyOnly:1b


// General utils

// Python dependencies

np:.p.import[`numpy]
pq:.p.import[`pyarrow.parquet]
io:.p.import`io
pa     :.p.import`pyarrow
patab  :.p.import[`pyarrow]`:Table
feather:.p.import`pyarrow.feather

// convert a pandas dataframe to a q table (this is a copy of .ml.df2tab)
/* x      = a pandas dataframe as a foreign
/. returns = a q table
dataframeToTable:{
 n:$[enlist[::]~x[`:index.names]`;0;x[`:index.nlevels]`];
 c:`$(x:$[n;x[`:reset_index][];x])[`:columns.get_values][]`;
 d:x[`:select_dtypes][`exclude pykw`datetime][`:to_dict;`list]`;
 d,:"p"$x[`:select_dtypes][`include pykw`datetime][`:astype;`int64][`:to_dict;<;`list]+1970.01.01D0;
 n!flip c#d}

// convert a q table to pandas dataframe (this is just a copy of .ml.tab2df)
/* x       = q table
/. returns = a pandas dataframe as a foreign object
tableToDataframe:{
 r:.p.import[`pandas;`:DataFrame;@[flip 0!x;i.fndcols[x]"pmdznuvt";i.q2npdt]][@;cols x];
 $[count k:keys x;r[`:set_index]k;r]}
i.q2npdt:{.p.import[`numpy;`:array;("p"$@[4#+["d"$0];-16+type x]x)-"p"$1970.01m;"datetime64[ns]"]`.}
i.fndcols:{m[`c]where(m:0!meta x)[`t]in y}



// handle an input path appropriately
/* x       = input path as sym, string, hsym or foreign
/. returns = the input modified appropriately pass through strings or foreigns unmodified
/            deal with strings as appropriate to type
parsePath:{tx:type x;$[-11h~tx;`$$[":"~first s:string x;1_s;s];`$$[":"~first s:string path;1_s;s]]}

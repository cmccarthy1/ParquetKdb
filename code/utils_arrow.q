\d .aw

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

// Function for the extraction of metadata from an arrow table
/* x       = arrow table
/. returns = metadata as a q dictionary
i.metadata:.p.get[`pyMetadata;<]


// convert an arrow table to a Pandas DataFrame
/* arrowTable = the arrow table to be converted
/. returns    = the Pandas DataFrame as a foreign object
i.toPandas:{[arrowTable]
  arrowTable[`:to_pandas]pykw[`zero_copy_only;i.zeroCopyOnly]
  }


// enforce a conversion to Pandas DataFrame (only when a copy is not being made)
i.zeroCopyOnly:1b

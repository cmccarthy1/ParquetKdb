\d .aw

// Python dependancies
pa     :.p.import`pyarrow
feather:.p.import`pyarrow.feather


// Read a feather file to obtain an Arrow table
/* target  = a symbol,string,hsym or foreign with the path to the feather file
/* opts    = dictionary indicating columns to read ,`columns!col_list or (::)
/. returns = an Arrow table
featherToArrow:{[target;opts]
  opts:$[(opts~(::)) or opts~()!();(enlist`)!enlist(::);(enlist[`]!enlist(::))^opts];
  if[-11h~type target;target:`$$[":"~first s:string target;1_s;s]];
  feather[`:read_table][target;opts`columns]
  }


// wrapper around the python function i.metadata
/* arrowTable = an arrow table
/. returns    = a dictionary containing the metadata `num_row_groups`num_rows`columns`types!JJSS
metadata:{[arrowTable]
  @[m;c;:;`$(m:i.metadata arrowTable)c:`columns`types]
  }



// Reads a feather file to obtain a kdb+ table
/* target  = the path to the feather file
/* opts    = dictionary indicating columns to read ,`columns!col_list or (::)
/. returns = the table from the feature file
readFeather:{[target;opts]
  readTable[;::;::;::] featherToArrow[target;opts]
  }


/// Refactor this
// converts an arrow table into a q table applying casts prior to transform
/* arrowTable = foreign of the table to be converted
/* batch      = optional argument to specifify which row group should be imported (long/null)
/* transforms = mapping of column names to functions to apply to the column (dict [symbol:fn])
/* casts      = special case of transforms to cast non-strings and tok strings as specified (dict [symbol: symbol])
/. returns    = q table
readTable:{[arrowTable;batch;transforms;casts]
  t:(transforms~(::))|transforms~()!();
  c:(casts~(::))|casts~()!();
  caster:{$[10h~type first y;upper[x]$y;x$y]};
  $[t;::;![;();0b;k!value[transforms],'k:key transforms]]
    $[c;::;![;();0b;k!caster,'(i.casts value casts),'k:key casts]]
    .ap.dataframeToTable i.toPandas $[batch~(::);arrowTable;arrowTable[`:to_batches][][@;batch]]
  }


// converts a q table to an arrow table
// t        = a q table
/. return = the arrow table as an EmbedPy object
toArrowtable:{[t]
  dt_cols:m[`c]where(m:0!meta t)[`t]in "pmdznuvt";
  d:flip t;
  d[dt_cols]:.ap.numpifyDatetime t dt_cols;
  pa[`:Table][`:from_pydict]d
  }


// write a kdb table or arrow table to disk in feather format
/* t       = the kdb+ table or arrow table to be written to disk
/* target  = the path or file object to write the table to as a sym, string, hsym or foreign
/* opts    = NYI here as a  placeholder but would be a dict 
/. returns = (::) -> should probably error out on failure?
writeFeather:{[t;target;opts]
  if[-11h~type target;target:`$$[":"~first s:string target;1_s;s]];
  feather[`:write_feather][;target] $[98h~type t;.ap.tableToDataframe t;t];
  }

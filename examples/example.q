\l ParquetKdb/parquet.q
.pq.loadfile`:init.q

base:"./data/userdata"

-1"\nImporting and displaying a single parquet file as a kdb table\n";
show 5#.pq.readTable[`$base,"/userdata1.parquet";::];

-1"\n\nReading the metadata associated with the single parquet file\n";
show .pq.metadata[`$base,"/userdata1.parquet"];

-1"\n\nImporting a partitioned parquet table to kdb\n";
show 5#.pq.readTable[`$base;::];

-1"\n\nReading the metadata associated with the partitioned parquet file\n";
show .pq.metadata[`$base];

-1"\n\nReading a subset of the columns within the parquet file \n";
columns:enlist[`columns]!enlist `first_name`last_name`ip_address
show 5#.pq.readTable[`$base;columns];

-1"\n\nApplying the following q operations to columns of a dataset on ingest\n";
show (transforms:enlist[`transforms]!enlist `gender`ip_address!({`$x};{"I"$x}))`transforms
-1"\nApplying the above functionality and displaying output as a dictionary\n";
show flip .pq.readTable[`$base;transforms];

-1"\n\nCasting data types of columns (this is a special case of the q operations above)\n";
show casts:enlist[`casts]!enlist `gender`ip_address!`symbol`symbol;
-1"\nThe following shows these casts as applied to the data";
show flip .pq.readTable[`$base;casts];

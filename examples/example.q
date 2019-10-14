\l ParquetKdb/parquet.q
.pq.loadfile`:init.q

base:"./data/userdata"

-1"\nImporting and displaying a single parquet file as a kdb table\n";
show 5#.pq.readParquetTable[`$base,"/userdata1.parquet";::];

-1"\n\nReading the metadata associated with the single parquet file\n";
show .pq.parquetMetadata[`$base,"/userdata1.parquet"];

-1"\n\nImporting a partitioned parquet table to kdb\n";
show 5#.pq.readParquetTable[`$base;::];

-1"\n\nReading the metadata associated with the partitioned parquet file\n";
show .pq.parquetMetadata[`$base];

-1"\n\nReading a subset of the columns within the parquet file \n";
columns:enlist[`columns]!enlist `first_name`last_name`ip_address
show 5#.pq.readParquetTable[`$base;columns];

-1"\n\nApplying the following q operations to columns of a dataset on ingest\n";
show (transforms:enlist[`transforms]!enlist `gender`ip_address!({`$x};{"I"$x}))`transforms
-1"\nApplying the above functionality and displaying output as a dictionary\n";
show flip .pq.readParquetTable[`$base;transforms];

-1"\n\nCasting data types of columns (this is a special case of the q operations above)\n";
show casts:enlist[`casts]!enlist `gender`ip_address!`symbol`symbol;
-1"\nThe following shows these casts as applied to the data";
show flip .pq.readParquetTable[`$base;casts];

-1"\n\nWriting the following table to a feather file\n";
show 5#tab:([]100?0b;100?1f;100?1f;100?("abc";"bcd"))
.aw.writeFeather[tab;`$"./test.feather";::];

-1"\n\nThis is the table as read from the feather file on disk\n";
show 5#.aw.readFeather[`$"./test.feather";::];

-1"\n\nConverting the saved feather table to arrow and returning the tables metadata\n";
f2a:.aw.featherToArrow[`$"./test.feather";::];
show .aw.arrowMetadata[f2a];

-1"\n\nReturning the arrow table converted from feather file as a q table\n";
show 5#.aw.readArrowTable[f2a;::;::;::];

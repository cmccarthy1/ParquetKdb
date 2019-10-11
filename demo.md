```q
base:"/DATA/user/will/parquet/";

// simple import of a single file
.axparquet.readTable[;::] `$base,"userdata/userdata1.parquet";

// simple import of a partitioned table
key hsym `$base,"userdata";
.axparquet.readTable[;::] `$base,"userdata";

// read metadata
.axparquet.metadata `$base,"userdata";

// import subset of columns
.axparquet.readTable[`$base,"userdata"] enlist[`columns]!enlist`first_name`last_name`ip_address;

// import batch of rows
.axparquet.readTable[`$base,"userdata"] enlist[`batch]!enlist 3; // import row-group #3

// apply functions to columns on import
.axparquet.readTable[`$base,"userdata"] enlist[`transforms]!enlist `gender`ip_address!(`$;"I"$);
// casts are a special case of transforms
.axparquet.readTable[`$base,"userdata"] enlist[`casts]!enlist `gender`ip_address!`symbol`int;

// write some taxi data
\l /DATA/demo/taxi/trips/2016.01
// 989 MB for yellow taxi data in this month
at:.axarrow.toArrowTable yt:select from yellow;
.axparquet.writeTable[at;`$base,"taxi.parq";::];
// 228 MB for the same data stored with default compression options
.axparquet.metadata `$base,"taxi.parq";
it:.axparquet.readTable[`$base,"taxi.parq";::]
meta it
meta yt
3 all/ it=yt

// write many random numbers
t:flip (`$'.Q.A)!26 10000000#(26*10000000)?9223372036854775807
.axparquet.writeTable[t;`$base,"randints.parq";::]

// TODO: write a partitioned table
.axparquet.arrowTableToPart

```
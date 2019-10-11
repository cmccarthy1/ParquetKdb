# Parquet and Arrow interface with kdb+

## Introduction

This repository contains a number of functions which are used to interogate, read and write to Apache Arrow and Parquet formats through embedPy

This repository contains the following subsections of code at present (contained in the `code/`)with currently three namespaces although this is only temporary:
*  `ax_apache.q`  contains functions for the conversion of q tables to pandas dataframes and vice-versa
*  `ax_arrow.q`   contains functions for the conversion of q tables to 'feather' files and arrow tables and vice-versa
*  `ax_parquet.q` contains functions for the conversion of q tables to parquet files on disk, for importing parquet files from disk, writing arrow tables to disk and converting a byte buffer to an arrow table


Documentation for this library will be added in time, for now it should be taken that the comments within the `.q` files is sufficient to test the functions as is the demo script provided


## Requirements

- embedPy

The python packages required to allow successful execution of all functions within the this interface can be installed via:

pip:
```bash
pip install -r requirements.txt
```

or via conda:
```bash
conda install --file requirements.txt
```

## Installation

Place the library file in `$QHOME` and load into a q instance using `parquet/parquet.q`

This will load all the functions contained within the `.pq`,`.aw` and `.ap` namespaces  
```q
q)\l parquet/parquet.q
q).pq.loadfile`:init.q
```

## Examples

The examples can be run from the `examples` folder through execution of `$q examples.q` this assumes that the setup above has been followed in particular the location in `$QHOME`

```
$q examples.q
Loading init.q
Loading code/utils_parquet.q
Loading code/utils_arrow.q
Loading code/ax_apache.q
Loading code/ax_arrow.q
Loading code/ax_parquet.q

Importing and displaying a single parquet file as a kdb table

registration_dttm             id first_name last_name email                  ..
-----------------------------------------------------------------------------..
2016.02.03D07:55:29.000000000 1  "Amanda"   "Jordan"  "ajordan0@com.com"     ..
2016.02.03D17:04:03.000000000 2  "Albert"   "Freeman" "afreeman1@is.gd"      ..
2016.02.03D01:09:31.000000000 3  "Evelyn"   "Morgan"  "emorgan2@altervista.or..
2016.02.03D00:36:21.000000000 4  "Denise"   "Riley"   "driley3@gmpg.org"     ..
2016.02.03D05:05:31.000000000 5  "Carlos"   "Burns"   "cburns4@miitbeian.gov...


Reading the metadata associated with the single parquet file

num_row_groups| 1
num_rows      | 1000
columns       | `registration_dttm`id`first_name`last_name`email`gender`ip_ad..
types         | `timestamp[ns]`int32`string`string`string`string`string`strin..


Importing a partitioned parquet table to kdb

registration_dttm             id first_name last_name email                  ..
-----------------------------------------------------------------------------..
2016.02.03D07:55:29.000000000 1  "Amanda"   "Jordan"  "ajordan0@com.com"     ..
2016.02.03D17:04:03.000000000 2  "Albert"   "Freeman" "afreeman1@is.gd"      ..
2016.02.03D01:09:31.000000000 3  "Evelyn"   "Morgan"  "emorgan2@altervista.or..
2016.02.03D00:36:21.000000000 4  "Denise"   "Riley"   "driley3@gmpg.org"     ..
2016.02.03D05:05:31.000000000 5  "Carlos"   "Burns"   "cburns4@miitbeian.gov...


Reading the metadata associated with the partitioned parquet file

num_row_groups| 5
num_rows      | 5000
columns       | `registration_dttm`id`first_name`last_name`email`gender`ip_ad..
types         | `timestamp[ns]`int32`string`string`string`string`string`strin..

Reading a subset of the columns within the parquet file 

first_name last_name ip_address      
-------------------------------------
"Amanda"   "Jordan"  "1.197.201.2"   
"Albert"   "Freeman" "218.111.175.34"
"Evelyn"   "Morgan"  "7.161.136.94"  
"Denise"   "Riley"   "140.35.109.83" 
"Carlos"   "Burns"   "169.113.235.40"


Applying the following q operations to columns of a dataset on ingest

gender    | {`$x}
ip_address| {"I"$x}

Applying the above functionality and displaying output as a dictionary

registration_dttm| 2016.02.03D07:55:29.000000000 2016.02.03D17:04:03.00000000..
id               | 1                             2                           ..
first_name       | "Amanda"                      "Albert"                    ..
last_name        | "Jordan"                      "Freeman"                   ..
email            | "ajordan0@com.com"            "afreeman1@is.gd"           ..
gender           | Female                        Male                        ..
ip_address       | 29739266                      -630214878                  ..
cc               | "6759521864920116"            ""                          ..
country          | "Indonesia"                   "Canada"                    ..
birthdate        | "3/8/1971"                    "1/16/1968"                 ..
salary           | 49756.53                      150280.2                    ..
title            | "Internal Auditor"            "Accountant IV"             ..
comments         | "1E+02"                       ""                          ..


Casting data types of columns (this is a special case of the q operations above)

     | gender ip_address
-----| -----------------
casts| symbol symbol    

The following shows these casts as applied to the data
registration_dttm| 2016.02.03D07:55:29.000000000 2016.02.03D17:04:03.00000000..
id               | 1                             2                           ..
first_name       | "Amanda"                      "Albert"                    ..
last_name        | "Jordan"                      "Freeman"                   ..
email            | "ajordan0@com.com"            "afreeman1@is.gd"           ..
gender           | Female                        Male                        ..
ip_address       | 1.197.201.2                   218.111.175.34              ..
cc               | "6759521864920116"            ""                          ..
country          | "Indonesia"                   "Canada"                    ..
birthdate        | "3/8/1971"                    "1/16/1968"                 ..
salary           | 49756.53                      150280.2                    ..
title            | "Internal Auditor"            "Accountant IV"             ..
comments         | "1E+02"                       ""                          ..
```

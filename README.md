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

# pq2xl

[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`pq2xl` is a simple command line tool for converting parquet files to xlsx or csv.

## Acknowledgements

This tool is a very simple interface, entirely powered by the following libraries:

- [polars](https://github.com/pola-rs/polars) an excellent library for manipulating tabular data. Used to read the input parquet files and convert data types not supported by xlsx.
- [polars_excel_writer](https://github.com/jmcnamara/polars_excel_writer) a library for serialising polars data frames to xlsx files.

The command line interface is build with [clap](https://github.com/clap-rs/clap).

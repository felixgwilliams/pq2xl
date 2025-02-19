# pq2xl

![Test](https://github.com/felixgwilliams/pq2xl/actions/workflows/testing.yml/badge.svg)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI - Version](https://img.shields.io/pypi/v/pq2xl)](https://pypi.org/project/pq2xl/)
[![Crates.io](https://img.shields.io/crates/v/pq2xl)](https://crates.io/crates/pq2xl)
[![codecov](https://codecov.io/gh/felixgwilliams/pq2xl/graph/badge.svg?token=4PQYOS6RZI)](https://codecov.io/gh/felixgwilliams/pq2xl)

`pq2xl` is a simple command line tool for converting parquet files to xlsx or csv.

```shell
pq2xl data.parquet -o data.xlsx
```

See all options in [CommandLineHelp.md](CommandLineHelp.md).

## Acknowledgements

This tool is a very simple interface, entirely powered by the following libraries:

- [polars](https://github.com/pola-rs/polars) an excellent library for manipulating tabular data. Used to read the input parquet files and convert data types not supported by xlsx.
- [polars_excel_writer](https://github.com/jmcnamara/polars_excel_writer) a library for serialising polars data frames to xlsx files.

The command line interface is build with [clap](https://github.com/clap-rs/clap).

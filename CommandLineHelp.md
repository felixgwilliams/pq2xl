# Command-Line Help for `pq2xl`

This document contains the help content for the `pq2xl` command-line program.

**Command Overview:**

* [`pq2xl`↴](#pq2xl)

## `pq2xl`

A simple command line tool for converting parquet files to xlsx or csv

**Usage:** `pq2xl [OPTIONS] <IN_FILE>`

###### **Arguments:**

* `<IN_FILE>` — path to input parquet file

###### **Options:**

* `-o`, `--out-file <OUT_FILE>` — path to output file. If not given, will use the input file name with a different extension
* `-f`, `--format <FORMAT>` — Specify output format csv/xlsx. If not given, infer from the output file name, falling back to xlsx

  Possible values: `xlsx`, `csv`

* `--lossy-action <LOSSY_ACTION>` — What to do if a data type is encountered whose conversion may be lossy. warn: emit warning. error: abort. allow: continue. Default: allow

  Possible values: `allow`, `warn`, `error`

* `--duration-format <DURATION_FORMAT>` — How to format duration columns. physical: underlying integer form (the unit will be printed in the shell) unit: Same as physical, but with the unit (ms, us, ns) appended. human: human-readable format Default: physical

  Possible values: `physical`, `unit`, `human`

<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

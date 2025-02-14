#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::style)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::multiple_crate_versions)]
#![allow(clippy::cargo)]
// #![warn(missing_docs)]

use std::fs::File;

use anyhow::{bail, Error};
use polars::prelude::*;
enum Conversion {
    Pass,
    Convert(DataType),
    Error(DataType),
    Process(DataType),
}

use polars_excel_writer::PolarsXlsxWriter;
fn map_supported(dtype: &DataType) -> Conversion {
    match dtype {
        DataType::UInt8 => Conversion::Pass,
        DataType::UInt16 => Conversion::Pass,
        DataType::UInt32 => Conversion::Pass,
        DataType::UInt64 => Conversion::Pass,
        DataType::Int8 => Conversion::Pass,
        DataType::Int16 => Conversion::Pass,
        DataType::Int32 => Conversion::Pass,
        DataType::Int64 => Conversion::Pass,
        DataType::Int128 => Conversion::Pass,
        DataType::Float32 => Conversion::Pass,
        DataType::Float64 => Conversion::Pass,
        DataType::String => Conversion::Pass,
        DataType::Null => Conversion::Pass,
        DataType::Date => Conversion::Pass,
        DataType::Datetime(_, _) => Conversion::Pass,
        DataType::Time => Conversion::Pass,
        DataType::Boolean => Conversion::Pass,
        // not supported
        DataType::Binary => Conversion::Convert(DataType::String),
        DataType::BinaryOffset => Conversion::Convert(DataType::String),
        DataType::Duration(_) => Conversion::Convert(DataType::String),
        #[cfg(feature = "polars-categorical")]
        DataType::Enum(_, _) => Conversion::Convert(DataType::String),
        #[cfg(feature = "polars-decimal")]
        DataType::Decimal(_, _) => Conversion::Convert(DataType::Float64),
        #[cfg(feature = "polars-categorical")]
        DataType::Categorical(_, _) => Conversion::Convert(DataType::String),
        // cannot convert
        #[cfg(feature = "polars-struct")]
        DataType::Struct(inner) => Conversion::Error(DataType::Struct(inner.clone())),
        DataType::Unknown(inner) => Conversion::Error(DataType::Unknown(*inner)),
        DataType::List(inner) => Conversion::Process(DataType::List(inner.clone())),
    }
}
fn process(c: Expr, dtype: &DataType) -> Result<Expr, Error> {
    match dtype {
        DataType::List(_) => Ok(process_list(c)),
        _ => bail!("Don't know how to process {dtype:?}"),
    }
}
fn process_list(c: Expr) -> Expr {
    let joined = c
        .cast(DataType::List(Box::new(DataType::String)))
        .list()
        .join(lit(","), false);
    format_str("[{}]", vec![joined])
        .expect("invalid format str")
        .name()
        .keep()
}

fn main() -> Result<(), Error> {
    // let convert_category = false;
    let pq_file = File::open("data/example_parquet.parquet")?;
    let mut df = ParquetReader::new(pq_file).set_rechunk(true).finish()?;

    let mut casts = vec![];
    for (col_num, column) in df.get_columns().iter().enumerate() {
        match map_supported(column.dtype()) {
            Conversion::Pass => {}
            Conversion::Convert(target) => {
                casts.push(nth(col_num.try_into()?).cast(target));
            }
            Conversion::Error(tt) => bail!("Unsupported data type: {tt:?}"),
            Conversion::Process(dtype) => casts.push(process(nth(col_num.try_into()?), &dtype)?),
        }
    }
    dbg!(&casts);
    if !casts.is_empty() {
        df = df.lazy().with_columns(casts).collect()?;
    }
    // Create a new Excel writer.
    let mut xlsx_writer = PolarsXlsxWriter::new();

    // Write the dataframe to Excel.
    xlsx_writer.write_dataframe(&df)?;

    // Save the file to disk.
    xlsx_writer.save("data/dataframe.xlsx")?;
    // let pq_file = File::create("dataframe.parquet")?;
    // ParquetWriter::new(pq_file).finish(&mut df)?;

    Ok(())
}

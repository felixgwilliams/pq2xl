use std::fs::File;

// use polars::prelude::*;
use polars::{error::PolarsResult, prelude::ParquetReader};
use polars::prelude::SerReader;

use polars_excel_writer::PolarsXlsxWriter;

fn main() -> PolarsResult<()> {
    // Create a sample dataframe for the example.
    // let mut df: DataFrame = df!(
    //     "String" => &["North", "South", "East", "West"],
    //     "Integer" => &[1, 2, 3, 4],
    //     "Float" => &[4.0, 5.0, 6.0, 7.0],
    //     "Time" => &[
    //         NaiveTime::from_hms_milli_opt(2, 59, 3, 456).unwrap(),
    //         NaiveTime::from_hms_milli_opt(2, 59, 3, 456).unwrap(),
    //         NaiveTime::from_hms_milli_opt(2, 59, 3, 456).unwrap(),
    //         NaiveTime::from_hms_milli_opt(2, 59, 3, 456).unwrap(),
    //         ],
    //     "Date" => &[
    //         NaiveDate::from_ymd_opt(2022, 1, 1).unwrap(),
    //         NaiveDate::from_ymd_opt(2022, 1, 2).unwrap(),
    //         NaiveDate::from_ymd_opt(2022, 1, 3).unwrap(),
    //         NaiveDate::from_ymd_opt(2022, 1, 4).unwrap(),
    //         ],
    //     "Datetime" => &[
    //         NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(1, 0, 0).unwrap(),
    //         NaiveDate::from_ymd_opt(2022, 1, 2).unwrap().and_hms_opt(2, 0, 0).unwrap(),
    //         NaiveDate::from_ymd_opt(2022, 1, 3).unwrap().and_hms_opt(3, 0, 0).unwrap(),
    //         NaiveDate::from_ymd_opt(2022, 1, 4).unwrap().and_hms_opt(4, 0, 0).unwrap(),
    //     ],
    // )?;
    let pq_file = File::open("data/example_parquet.parquet")?;
    let df = ParquetReader::new(pq_file).set_rechunk(true).finish()?;

    // dbg!(&df);

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
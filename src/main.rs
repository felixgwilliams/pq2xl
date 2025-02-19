#![warn(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::style,
    clippy::cargo,
    rustdoc::all
)]
#![warn(clippy::unwrap_used)]
// comment to see any multiple versions for core deps
#![allow(clippy::multiple_crate_versions)]
// #![allow(clippy::cargo)]
#![warn(missing_docs)]

//! Application to convert parquet to Excel or CSV.
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{bail, Error};
use clap::{
    builder::{styling::AnsiColor, Styles},
    command, Parser, ValueEnum,
};
use polars::prelude::*;
use polars_excel_writer::PolarsXlsxWriter;

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Debug, Clone)]
enum Conversion<'a> {
    Pass,
    Convert(DataType),
    Error(&'a DataType),
    Process(&'a DataType),
    Lossy(&'a DataType),
}

const fn map_supported(dtype: &DataType) -> Conversion {
    match dtype {
        // essentially lossless conversion
        DataType::UInt8 => Conversion::Pass,
        DataType::UInt16 => Conversion::Pass,
        DataType::UInt32 => Conversion::Pass,
        DataType::Int8 => Conversion::Pass,
        DataType::Int16 => Conversion::Pass,
        DataType::Int32 => Conversion::Pass,
        DataType::Float32 => Conversion::Pass,
        DataType::String => Conversion::Pass,
        DataType::Null => Conversion::Pass,
        DataType::Boolean => Conversion::Pass,
        DataType::Float64 => Conversion::Pass,
        // lossy temporal conversion
        DataType::Date => Conversion::Lossy(dtype),
        DataType::Datetime(_, _) => Conversion::Lossy(dtype),
        DataType::Time => Conversion::Lossy(dtype),
        // lossy conversion (excel uses 64 bit floats everywhere)
        DataType::Int128 => Conversion::Lossy(dtype),
        DataType::UInt64 => Conversion::Lossy(dtype),
        DataType::Int64 => Conversion::Lossy(dtype),

        // not supported by polars_xlsxwriter
        DataType::Binary => Conversion::Convert(DataType::String),
        #[cfg(feature = "polars-categorical")]
        DataType::Enum(_, _) => Conversion::Convert(DataType::String),
        #[cfg(feature = "polars-decimal")]
        DataType::Decimal(_, _) => Conversion::Convert(DataType::Float64),
        #[cfg(feature = "polars-categorical")]
        DataType::Categorical(_, _) => Conversion::Convert(DataType::String),
        // cannot convert
        #[cfg(feature = "polars-struct")]
        DataType::Struct(_) => Conversion::Error(dtype),
        DataType::Unknown(_) => Conversion::Error(dtype),
        // can convert in a somewhat lossy way
        DataType::List(_) => Conversion::Process(dtype),
        DataType::Duration(_) => Conversion::Process(dtype),
        // not sure what this is
        DataType::BinaryOffset => Conversion::Error(dtype),
    }
}

#[derive(Debug, Clone, ValueEnum, Copy, Default)]
enum LossyAction {
    #[default]
    Allow,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
struct ConvertOptions {
    lossy_action: LossyAction,
    duration_format: DurationFormat,
}

fn process(c: Expr, dtype: &DataType, options: &ConvertOptions) -> Result<Expr, Error> {
    lossy_action(dtype, options)?;
    match dtype {
        DataType::List(_) => Ok(process_list(c, options)),
        DataType::Duration(timeunit) => Ok(process_duration(c, *timeunit, options)),
        _ => bail!("Don't know how to process {dtype:?}"),
    }
}
fn process_list(c: Expr, _options: &ConvertOptions) -> Expr {
    let joined = c
        .cast(DataType::List(Box::new(DataType::String)))
        .list()
        .join(lit(","), false);
    format_str("[{}]", vec![joined])
        .expect("invalid format str")
        .name()
        .keep()
}

fn easy_name(c: &Expr) -> Result<String, Error> {
    match c {
        // Expr::Column(name) => Ok(format!("\"{name}\"")),
        Expr::Nth(ind) => Ok(format!("{ind}")),
        _ => bail!("Unknown col name"),
    }
}
fn process_duration(c: Expr, timeunit: TimeUnit, options: &ConvertOptions) -> Expr {
    // dbg!(&c);

    match options.duration_format {
        DurationFormat::Physical => {
            eprintln!(
                "Duration column {} will be converted to number of {}",
                easy_name(&c).as_deref().unwrap_or("[UNKNOWN]"),
                timeunit
            );
            c.to_physical().name().keep()
        }
        DurationFormat::Unit => {
            let formatstr = format!("{{}}{timeunit}");
            format_str(&formatstr, vec![c.to_physical()])
                .expect("invalid format string")
                .name()
                .keep()
        } // DurationFormat::Human => todo!(),
    }
}
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
enum OutFormat {
    #[default]
    Xlsx,
    Csv,
}
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum DurationFormat {
    #[default]
    Physical,
    Unit,
    // Human,
}

// uses an idea from https://jwodder.github.io/kbits/posts/clap-bool-negate/

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None, styles=STYLES)]
struct Cli {
    #[cfg(feature = "markdown-help")]
    #[arg(long, hide = true)]
    pub markdown_help: bool,

    /// path to input parquet file
    in_file: PathBuf,

    /// path to output file. If not given, will use the input file name with a different extension
    #[arg(long, short)]
    out_file: Option<PathBuf>,

    /// Specify output format csv/xlsx. If not given, infer from the output file name, falling back to xlsx.
    #[arg(long, short)]
    format: Option<OutFormat>,

    /// What to do if a data type is encountered whose conversion may be lossy. warn: emit warning. error: abort. allow: continue. Default: allow.
    #[arg(long)]
    lossy_action: Option<LossyAction>,
    /// How to format duration columns.
    ///     physical: underlying integer form (the unit will be printed in the shell)
    ///     unit: Same as physical, but with the unit (ms, us, ns) appended.
    ///     Default: physical
    #[arg(long)]
    duration_format: Option<DurationFormat>,
}

fn lossy_action(dtype: &DataType, options: &ConvertOptions) -> Result<(), Error> {
    match options.lossy_action {
        LossyAction::Allow => {}
        LossyAction::Warn => {
            eprintln!("Warning conversion of {dtype} is lossy");
        }
        LossyAction::Error => {
            bail!("Conversion of {dtype} is lossy. Aborting")
        }
    }
    Ok(())
}

fn get_conversions(df: &DataFrame, convert_options: &ConvertOptions) -> Result<Vec<Expr>, Error> {
    let mut casts = vec![];
    for (col_num, column) in df.get_columns().iter().enumerate() {
        match map_supported(column.dtype()) {
            Conversion::Pass => {}
            Conversion::Lossy(dtype) => lossy_action(dtype, convert_options)?,
            Conversion::Convert(target) => {
                lossy_action(column.dtype(), convert_options)?;
                casts.push(nth(col_num.try_into()?).cast(target.clone()));
            }
            Conversion::Error(tt) => bail!("Unsupported data type: {tt:?}"),
            Conversion::Process(dtype) => {
                casts.push(process(nth(col_num.try_into()?), dtype, convert_options)?);
            }
        }
    }
    Ok(casts)
}

fn process_df(df: DataFrame, convert_options: &ConvertOptions) -> Result<DataFrame, Error> {
    let casts = get_conversions(&df, convert_options)?;
    // dbg!(&casts);
    if casts.is_empty() {
        Ok(df)
    } else {
        Ok(df.lazy().with_columns(casts).collect()?)
    }
}

fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    #[cfg(feature = "markdown-help")]
    if cli.markdown_help {
        #[cfg(not(tarpaulin_include))]
        clap_markdown::print_help_markdown::<Cli>();
        return Ok(());
    }

    // let convert_category = false;
    let pq_file = File::open(&cli.in_file)?;
    let mut df = ParquetReader::new(pq_file).set_rechunk(true).finish()?;

    let convert_options = ConvertOptions {
        lossy_action: cli.lossy_action.unwrap_or_default(),
        duration_format: cli.duration_format.unwrap_or_default(),
    };
    df = process_df(df, &convert_options)?;
    match cli
        .format
        .unwrap_or_else(|| format_from_file(cli.out_file.as_deref()).unwrap_or_default())
    {
        OutFormat::Csv => {
            let out_file = cli
                .out_file
                .clone()
                .unwrap_or_else(|| cli.in_file.with_extension("csv"));
            let writer = File::create(out_file)?;
            CsvWriter::new(writer).finish(&mut df)?;
        }
        OutFormat::Xlsx => {
            // Create a new Excel writer.

            let mut xlsx_writer = PolarsXlsxWriter::new();

            // Write the dataframe to Excel.
            xlsx_writer.write_dataframe(&df)?;

            // Save the file to disk.
            let out_file = cli
                .out_file
                .clone()
                .unwrap_or_else(|| cli.in_file.with_extension("xlsx"));

            xlsx_writer.save(out_file)?;
        }
    }

    Ok(())
}

fn format_from_file<P: AsRef<Path>>(p: Option<P>) -> Option<OutFormat> {
    p.and_then(|p| {
        let ext = p.as_ref().extension()?.to_str()?;
        match ext {
            "xlsx" => Some(OutFormat::Xlsx),
            "csv" => Some(OutFormat::Csv),
            _ => None,
        }
    })
}
#[cfg(test)]
mod test {
    use super::*;
    use chrono::prelude::*;
    #[test]
    fn test_format() {
        assert_eq!(format_from_file(None::<&Path>), None);
        assert_eq!(
            format_from_file(Some(&PathBuf::from("example.xlsx"))),
            Some(OutFormat::Xlsx)
        );
        assert_eq!(
            format_from_file(Some(&PathBuf::from("example.csv"))),
            Some(OutFormat::Csv)
        );
        assert_eq!(format_from_file(Some(&PathBuf::from("example.mp3"))), None);
    }

    fn make_no_loss_df() -> DataFrame {
        DataFrame::new(vec![
            Column::new("strings".into(), vec!["a", "b", "c"]),
            // Column::new("u8".into(), &[1u32, 2u32, 3u32]),
            // Column::new("u16".into(), &[1u32, 2u32, 3u32]),
            Column::new("i16".into(), &[1i16, 2, 3]),
            Column::new("u32".into(), &[1u32, 2, 3]),
            Column::new("i32".into(), &[1i32, 2, 3]),
            Column::new("i8".into(), &[1i8, 2, 3]),
            Column::new("u8".into(), &[1u8, 2, 3]),
            Column::new("u16".into(), &[1u16, 2, 3]),
            // Column::new("u16".into(), vec![1u16, 2u16, 3u16]),
            Column::new("f32".into(), &[0.5f32, 0.25, 0.125]),
            Column::new("f64".into(), &[0.5f64, 0.25, 0.125]),
            Column::new("bool".into(), &[true, false, true]),
            Column::full_null("null".into(), 3, &DataType::Null),
        ])
        .expect("invalid df")
    }
    #[test]
    fn test_process_no_loss() {
        let df = make_no_loss_df();
        let options = ConvertOptions {
            lossy_action: LossyAction::Error,
            duration_format: DurationFormat::Physical,
        };
        let casts = get_conversions(&df, &options).expect("get casts failed");
        assert!(casts.is_empty());
        let proc_df = process_df(df.clone(), &options).expect("process failed");
        assert!(df.equals_missing(&proc_df));
    }

    fn make_lossy_df() -> DataFrame {
        let list_series = vec![
            // Each Series represents a row's list entries
            Series::new("".into(), &[1i32, 2, 3]), // Row 1: [1, 2, 3]
            Series::new("".into(), &[4i32]),       // Row 2: [4]
            Series::new("".into(), &[5i32, 6]),    // Row 3: [5, 6]
        ]
        .into_iter()
        .collect::<ListChunked>()
        .into_series(); // Convert to a Series
        let cat_column = Column::new("categorical".into(), vec!["a", "b", "c"])
            .cast(&DataType::Categorical(None, CategoricalOrdering::Physical))
            .expect("cast failed");
        let DataType::Categorical(x, y) = cat_column
            .as_series()
            .expect("could not extract series")
            .dtype()
            .clone()
        else {
            panic!()
        };
        DataFrame::new(vec![
            cat_column,
            Column::new("enum".into(), vec!["a", "b", "c"])
                .cast(&DataType::Categorical(None, CategoricalOrdering::Physical))
                .expect("cast failed")
                .cast(&DataType::Enum(x, y))
                .expect("cast failed"),
            Column::new("u64".into(), vec![1u64, 2, 3]),
            Column::new("i64".into(), vec![1i64, 2, 3]),
            Column::new("i128".into(), vec![1i64, 2, 3]),
            Column::new("duration".into(), vec![1i32, 2, 3]),
            Column::new("list".into(), list_series),
            Column::new(
                "time".into(),
                vec![
                    NaiveTime::from_hms_milli_opt(2, 59, 3, 456).expect("invalid time"),
                    NaiveTime::from_hms_milli_opt(3, 59, 3, 456).expect("invalid time"),
                    NaiveTime::from_hms_milli_opt(4, 59, 3, 456).expect("invalid time"),
                ],
            ),
            Column::new(
                "date".into(),
                vec![
                    NaiveDate::from_ymd_opt(2022, 1, 1).expect("invalid date"),
                    NaiveDate::from_ymd_opt(2022, 1, 2).expect("invalid date"),
                    NaiveDate::from_ymd_opt(2022, 1, 3).expect("invalid date"),
                ],
            ),
            Column::new(
                "datetime".into(),
                vec![
                    NaiveDate::from_ymd_opt(2022, 1, 1)
                        .expect("invalid date")
                        .and_hms_opt(1, 0, 0)
                        .expect("invalid time"),
                    NaiveDate::from_ymd_opt(2022, 1, 2)
                        .expect("invalid date")
                        .and_hms_opt(2, 0, 0)
                        .expect("invalid time"),
                    NaiveDate::from_ymd_opt(2022, 1, 3)
                        .expect("invalid date")
                        .and_hms_opt(3, 0, 0)
                        .expect("invalid time"),
                ],
            ),
            Column::new("binary".into(), vec!["a", "b", "c"])
                .cast(&DataType::Binary)
                .expect("cast failed"),
            Column::new("decimal".into(), vec![1.61, 2.71, 1.41])
                .cast(&DataType::Decimal(Some(3), Some(5)))
                .expect("cast failed"),
        ])
        .expect("invalid df")
        .lazy()
        .with_columns(vec![
            col("i128").cast(DataType::Int128),
            col("duration").cast(DataType::Duration(TimeUnit::Milliseconds)),
        ])
        .collect()
        .expect("collect failed")
    }
    #[test]
    fn test_process_lossy() {
        let df = make_lossy_df();
        dbg!(&df.schema());
        let options = ConvertOptions {
            lossy_action: LossyAction::Error,
            duration_format: DurationFormat::Physical,
        };
        let res = get_conversions(&df, &options);
        assert!(res.is_err());
        let options2 = ConvertOptions {
            lossy_action: LossyAction::Allow,
            duration_format: DurationFormat::Physical,
        };
        let casts = get_conversions(&df, &options2).expect("get casts failed");

        // need to do 5 casts
        // enum + cat + duration + list + binary + decimal
        assert_eq!(casts.len(), 6);
    }

    #[test]
    fn test_duration_cast() {
        let df_durations = DataFrame::new(vec![
            Column::new("duration_us".into(), vec![1i64, 2, 3])
                .cast(&DataType::Duration(TimeUnit::Microseconds))
                .expect("cast failed"),
            Column::new("duration_ms".into(), vec![1i64, 2, 3])
                .cast(&DataType::Duration(TimeUnit::Milliseconds))
                .expect("cast failed"),
            Column::new("duration_ns".into(), vec![1i64, 2, 3])
                .cast(&DataType::Duration(TimeUnit::Nanoseconds))
                .expect("cast failed"),
        ])
        .expect("df failed");
        let df_durations_physical = DataFrame::new(vec![
            Column::new("duration_us".into(), vec![1i64, 2, 3]),
            Column::new("duration_ms".into(), vec![1i64, 2, 3]),
            Column::new("duration_ns".into(), vec![1i64, 2, 3]),
        ])
        .expect("df failed");
        let df_durations_unit = DataFrame::new(vec![
            Column::new("duration_us".into(), vec!["1μs", "2μs", "3μs"]),
            Column::new("duration_ms".into(), vec!["1ms", "2ms", "3ms"]),
            Column::new("duration_ns".into(), vec!["1ns", "2ns", "3ns"]),
        ])
        .expect("df failed");
        let df_durations_physical_comp = process_df(
            df_durations.clone(),
            &ConvertOptions {
                lossy_action: LossyAction::Allow,
                duration_format: DurationFormat::Physical,
            },
        )
        .expect("process failed");
        let df_durations_unit_comp = process_df(
            df_durations,
            &ConvertOptions {
                lossy_action: LossyAction::Allow,
                duration_format: DurationFormat::Unit,
            },
        )
        .expect("process failed");
        assert!(df_durations_physical.equals(&df_durations_physical_comp));
        assert!(df_durations_unit.equals(&df_durations_unit_comp));
    }
}

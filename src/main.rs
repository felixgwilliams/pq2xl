#![warn(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::style,
    clippy::cargo
)]
#![warn(clippy::unwrap_used)]
// comment to see any multiple versions for core deps
#![allow(clippy::multiple_crate_versions)]
// #![allow(clippy::cargo)]
// #![warn(missing_docs)]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{bail, Error};
use clap::{
    builder::{styling::AnsiColor, Styles},
    command, ArgAction, Parser, ValueEnum,
};
use polars::prelude::*;
// use polars_core::fmt::fmt_duration_string;
// use polars_core::fmt::iso_duration_string;

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Debug, Clone)]
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
        // can convert in a somewhat lossy way
        DataType::List(inner) => Conversion::Process(DataType::List(inner.clone())),
        DataType::Duration(timeunit) => Conversion::Process(DataType::Duration(*timeunit)),
    }
}
#[derive(Debug, Clone)]
struct ConvertOptions {}

fn process(c: Expr, dtype: &DataType, options: &ConvertOptions) -> Result<Expr, Error> {
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
        Expr::Column(name) => Ok(format!("\"{name}\"")),
        Expr::Nth(ind) => Ok(format!("{ind}")),
        _ => bail!("Unknown col name"),
    }
}
fn process_duration(c: Expr, timeunit: TimeUnit, _options: &ConvertOptions) -> Expr {
    // dbg!(&c);

    eprintln!(
        "Duration column {} will be converted to number of {}",
        easy_name(&c).as_deref().unwrap_or("[UNKNOWN]"),
        timeunit
    );
    c.to_physical().name().keep()
    // let formatstr = format!("{{}}{timeunit}");
    // format_str(&formatstr, vec![c.to_physical()])
    //     .expect("invalid format string")
    //     .name()
    //     .keep()
}
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum OutFormat {
    #[default]
    Xlsx,
    Csv,
}
// uses an idea from https://jwodder.github.io/kbits/posts/clap-bool-negate/

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None, styles=STYLES)]
struct Cli {
    #[cfg(feature = "markdown-help")]
    #[arg(long, hide = true)]
    pub markdown_help: bool,

    in_file: PathBuf,

    #[arg(long, short)]
    out_file: Option<PathBuf>,

    #[arg(long = "no-coerce", action=ArgAction::SetFalse)]
    coerce: bool,

    #[arg(long = "coerce", overrides_with = "coerce")]
    _no_coerce: bool,

    #[arg(long, short)]
    format: Option<OutFormat>,
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

    let mut casts = vec![];
    if cli.coerce {
        let convert_options = ConvertOptions {};
        for (col_num, column) in df.get_columns().iter().enumerate() {
            match map_supported(column.dtype()) {
                Conversion::Pass => {}
                Conversion::Convert(target) => {
                    casts.push(nth(col_num.try_into()?).cast(target));
                }
                Conversion::Error(tt) => bail!("Unsupported data type: {tt:?}"),
                Conversion::Process(dtype) => {
                    casts.push(process(nth(col_num.try_into()?), &dtype, &convert_options)?);
                }
            }
        }
    }
    // dbg!(&casts);
    if !casts.is_empty() {
        df = df.lazy().with_columns(casts).collect()?;
    }
    match cli
        .format
        .unwrap_or_else(|| format_from_file(cli.out_file.as_deref()).unwrap_or_default())
    {
        OutFormat::Csv => todo!(),
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

fn format_from_file(p: Option<&Path>) -> Option<OutFormat> {
    let ext = p.and_then(Path::extension);
    match ext.and_then(|e| e.to_str()) {
        Some("xlsx") => Some(OutFormat::Xlsx),
        Some("csv") => Some(OutFormat::Csv),
        _ => None,
    }
}

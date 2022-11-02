use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

use byte_pair_encoding::sample::Sample;
use clap::Parser;
use zstd::Decoder;

#[derive(Parser)]
struct Args {
    input: PathBuf,
    output: PathBuf,

    #[clap(long)]
    max_samples: Option<usize>,
    #[clap(long)]
    max_bytes: Option<usize>,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    assert_ne!(args.input, args.output);

    let mut reader = BufReader::new(Decoder::new(File::open(&args.input)?)?);
    let mut writer = BufWriter::new(File::create(&args.output)?);

    let mut line = String::new();

    let mut samples = 0;
    let mut bytes = 0;
    let mut lines = 0;

    while below_limit(samples, args.max_samples) && below_limit(bytes, args.max_bytes) {
        line.clear();
        reader.read_line(&mut line)?;

        if line.is_empty() {
            // EOF reached
            break;
        }

        let sample: Sample = serde_json::from_str(&line)?;

        writer.write_all(sample.text.as_bytes())?;

        samples += 1;
        bytes += sample.text.len();
        lines += sample.text.lines().count();
    }

    writer.flush()?;

    println!("Decompressed {samples} samples, {lines} lines, {bytes} bytes");

    Ok(())
}

fn below_limit(x: usize, max: Option<usize>) -> bool {
    max.map_or(true, |max| x < max)
}

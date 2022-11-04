use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;

use kt_core::sample::SampleReader;

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

    let file = File::open(&args.input)?;
    let mut writer = BufWriter::new(File::create(&args.output)?);

    let mut samples = 0;
    let mut bytes = 0;
    let mut lines = 0;

    for sample in SampleReader::new_decode(file, true)? {
        let sample = sample?;

        writer.write_all(sample.text.as_bytes())?;

        samples += 1;
        bytes += sample.text.len();
        lines += sample.text.lines().count();

        if !below_limit(samples, args.max_samples) || !below_limit(bytes, args.max_bytes) {
            break;
        }
    }

    writer.flush()?;

    println!("Decompressed {samples} samples, {lines} lines, {bytes} bytes");

    Ok(())
}

fn below_limit(x: usize, max: Option<usize>) -> bool {
    max.map_or(true, |max| x < max)
}

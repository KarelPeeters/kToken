use std::fs::File;
use std::io::{BufRead, BufReader};

use byte_pair_encoding::{str_is_ltr, Sample};
use itertools::izip;
use serde::Deserialize;
use unicode_bidi::{bidi_class, BidiClass};
use unicode_normalization::UnicodeNormalization;
use zstd::stream::read::Decoder;

fn main() -> std::io::Result<()> {
    let path = r"\\192.168.0.10\Documents\Download\the-pile\00.jsonl.zst";
    // let path = r"\\192.168.0.10\Documents\Download\the-pile\test.jsonl.zst";

    let mut reader = BufReader::new(Decoder::new(File::open(path)?)?);

    // let mut writer_concatenated = BufWriter::new(File::create("ignored/concatenated.txt")?);
    // let mut writer_decompressed = BufWriter::new(File::create("ignored/decompressed.txt")?);
    // let mut buffer = vec![0; 1024*1024];
    // reader.read_exact(&mut buffer)?;
    // writer.write_all(&buffer)?;
    // writer.flush()?;
    // return Ok(());

    let mut line = String::new();
    let mut text = String::new();

    let mut sample_count = 0;
    let mut diff_sample_count = 0;
    let mut rtl_sample_count = 0;

    loop {
        line.clear();
        reader.read_line(&mut line)?;

        let sample: Sample = serde_json::from_str(&line)?;

        text.clear();
        text.extend(sample.text.nfc());

        sample_count += 1;

        if !str_is_ltr(&text) {
            rtl_sample_count += 1;

            let line = sample.text.lines().next().unwrap();
            if line.len() < 1024 && !str_is_ltr(line) {
                println!("RTL text found:");
                println!("  {}", text);
                println!("  {:?}", text.as_bytes());
            }
        }

        if sample.text != text {
            diff_sample_count += 1;

            let diff_line = izip!(sample.text.lines(), text.lines())
                .find(|(a, b)| a != b)
                .unwrap();

            if diff_line.0.len() < 1024 {
                println!("Line mismatch:");
                println!("  {}", diff_line.0);
                println!("  {}", diff_line.1);
                println!("  {:?}", diff_line.0.as_bytes());
                println!("  {:?}", diff_line.1.as_bytes());
            }
        }

        if sample_count % 10_000 == 0 {
            println!(
                "Got {sample_count} samples, {diff_sample_count} diff, {rtl_sample_count} rtl"
            );
        }
    }
}

extern crate core;

use std::fs::File;
use std::io::BufReader;

use aho_corasick::{AhoCorasickBuilder, MatchKind};
use serde::Deserialize;
use zstd::Decoder;

use kt_core::batch::Batcher;
use kt_core::sample::SampleReader;

#[derive(Debug, Deserialize)]
struct Tokens {
    tokens: Vec<Vec<u8>>,
}

fn main() -> std::io::Result<()> {
    // let path = r"C:\Users\Karel\Desktop\the-pile\00.jsonl.zst";
    let path = r"C:\Users\Karel\Desktop\the-pile\test.jsonl.zst";
    let path_tokens = "ignored/tokens.json";

    let batch_size = 4;
    let seq_len = 8;
    let mix_bucket_count = 2 * batch_size;

    let all_tokens: Tokens = serde_json::from_str(&std::fs::read_to_string(path_tokens)?)?;
    let aho = AhoCorasickBuilder::new()
        .match_kind(MatchKind::LeftmostLongest)
        .dfa(true)
        .build(&all_tokens.tokens);

    let mut batcher = Batcher::new(batch_size, seq_len, mix_bucket_count, aho);
    let reader = BufReader::new(Decoder::new(File::open(path)?)?);

    for sample in SampleReader::new(reader, true) {
        let sample = sample?;

        batcher.push_sample(&sample.text);

        // yield as many batches as possible
        while let Some(batch) = batcher.pop_batch() {
            println!(
                "Yielding batch with\n    shape {:?},\n    samples {:?},\n    start_indices {:?}",
                batch.tokens.shape(),
                batch.samples,
                batch.start_indices,
            );

            if batcher.stats().batch_count >= 10 {
                break;
            }
        }

        if batcher.stats().batch_count >= 10 {
            break;
        }
    }

    Ok(())
}

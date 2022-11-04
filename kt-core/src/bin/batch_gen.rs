extern crate core;

use std::cmp::min;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

use aho_corasick::{AhoCorasickBuilder, MatchKind};
use kt_core::sample::SampleReader;
use ndarray::Array2;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use zstd::Decoder;

#[derive(Debug, Deserialize)]
struct Tokens {
    tokens: Vec<Vec<u8>>,
}

struct Batch {
    tokens: Array2<usize>,
    samples: Vec<usize>,
    start_indices: Vec<usize>,
}

struct Bucket {
    // which sample index this data originated from
    sample: usize,
    // which token is currently at the front
    start_index: usize,

    // random number of tokens at the start has already been dropped
    tokens: VecDeque<usize>,
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

    let reader = BufReader::new(Decoder::new(File::open(path)?)?);
    let mut rng = SmallRng::seed_from_u64(0);
    let start = Instant::now();

    let mut batch_count: usize = 0;
    let mut sample_count: usize = 0;
    let mut token_count: usize = 0;

    // currently non-empty buffers
    let mut non_empty_buckets = VecDeque::new();
    // old empty buffers that can be reused to minimize allocations
    let mut empty_buffers = VecDeque::new();

    for sample in SampleReader::new(reader, true) {
        let sample = sample?;

        // println!(
        //     "Samples: {}, buckets: non-empty {}, empty {}",
        //     sample_count,
        //     non_empty_buckets.len(),
        //     empty_buckets.len()
        // );

        // pick a bucket to put the sequences into

        let mut buffer = empty_buffers.pop_front().unwrap_or_else(VecDeque::default);
        assert!(buffer.is_empty(), "Empty buffer is non-empty");

        // tokenize straight into bucket
        buffer.extend(aho.find_iter(&sample.text).map(|m| m.pattern()));
        assert!(!buffer.is_empty(), "Newly filled buffer is empty",);
        let parsed_token_count = buffer.len();

        // TODO should we allow dropping more, even if that causes non-full sequences?
        // drop random tokens at the start
        //   leftover elements that are not a multiple of seq_len will be dropped later
        let offset = if buffer.len() > seq_len {
            rng.gen_range(0..seq_len)
        } else {
            0
        };
        drop(buffer.drain(0..offset));

        let bucket = Bucket {
            sample: sample_count,
            start_index: offset,
            tokens: buffer,
        };
        non_empty_buckets.push_back(bucket);

        // update stats
        sample_count += 1;
        token_count += parsed_token_count;

        // yield as many batches as possible
        while non_empty_buckets.len() > mix_bucket_count {
            // TODO what special value to use here? ::MAX might overflow to -1 and be weird in pytorch
            let mut batch = Array2::zeros((batch_size, seq_len));
            batch.fill(usize::MAX);
            let mut samples = vec![];
            let mut start_indices = vec![];

            for bi in 0..batch_size {
                // TODO we might sample multiple times from the same bucket, is that a problem?
                // pick a random non-empty buffer
                let bucket_index = rng.gen_range(0..non_empty_buckets.len());
                let bucket = &mut non_empty_buckets[bucket_index];
                samples.push(bucket.sample);
                start_indices.push(bucket.start_index);

                // we can initially get less tokens than seq_len, that just means the sample was short, keep it
                let curr_seq_len = min(seq_len, bucket.tokens.len());
                assert!(curr_seq_len > 0, "Non-empty bucket is empty");

                // copy tokens into batch (and remove from buffer)
                let drain = bucket.tokens.drain(0..curr_seq_len);
                for (i, token) in drain.enumerate() {
                    batch[(bi, i)] = token;
                }
                bucket.start_index += curr_seq_len;

                // remove potentially empty buffer
                // if we have less than seq_len tokens left at this point they're just overflow, drop them
                //   (they could have been sampled because of the offset, we're not introducing bias here)
                if bucket.tokens.len() < seq_len {
                    bucket.tokens.clear();
                    empty_buffers.push_back(non_empty_buckets.remove(bucket_index).unwrap().tokens);
                }
            }

            println!(
                "Yielding batch with\n    shape {:?},\n    samples {:?},\n    start_indices {:?}",
                batch.shape(),
                samples,
                start_indices,
            );
            batch_count += 1;

            if batch_count > 100 {
                break;
            }
        }

        if batch_count > 100 {
            break;
        }
    }

    let delta = start.elapsed().as_secs_f32();
    let batches = token_count as f32 / (batch_size * seq_len) as f32;
    let tp = batches / delta;

    println!("Samples {}, tokens {}", sample_count, token_count);
    println!("Throughput: {} batches/s", tp);

    Ok(())
}

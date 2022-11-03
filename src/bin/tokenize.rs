use std::cmp::min;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::time::Instant;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use itertools::Itertools;
use ndarray::{s, Array2};
use zstd::Decoder;

use byte_pair_encoding::iter::FlatRepeatResult;
use byte_pair_encoding::sample::SampleReader;

// TODO remove tokens that are no longer used since they became part of the larger token?
//    eg. maybe we don't need "havi" any more after we have "having"
// TODO prevent merging between punctuation and word characters (and digits?)
//    eg. "tion." is not great
//    be careful to still allow things like "we'll"?
// TODO merge space into word like in GPT? (allowing us to almost halve the amount of tokens for a given sentence!)
//    eg  ["hello", " ", "there"] vs ["hello_", "there"]
//    maybe think of tokens as continuation by default, eg. ["hel", "_lo"]
//    and the rule is "every token that does not start with "_" and is not preceded by a whitespace token was preceded by a single space
//    meh, all of this is messy, ad-hoc and most importantly non-zero!

type Count = u32;

fn main() -> std::io::Result<()> {
    // let path = r"C:\Users\Karel\Desktop\the-pile\test.jsonl.zst";
    // let path = r"\\192.168.0.10\Documents\Download\the-pile\00.jsonl.zst";
    let path = r"C:\Users\Karel\Desktop\the-pile\00.jsonl.zst";
    let vocab_path = r"ignored/vocab.json";

    let max_tokens = 1 * 1024;
    let count_threshold = 10_000;
    let samples_threshold = 200;
    let count_decay_numerator: u32 = 98;
    let count_decay_denominator: u32 = 100;

    assert!(count_threshold < Count::MAX);

    let mut tokens = (0..u8::MAX).map(|x| vec![x]).collect_vec();
    let mut is_whitespace = (0..u8::MAX)
        .map(|c| (c as char).is_whitespace())
        .collect_vec();

    let mut tokens_since_add = 0;
    let mut samples_since_add = 0;
    let mut top_count = 0;
    let mut top_index = None;
    let mut bigram_count: Array2<Count> = Array2::zeros((max_tokens, max_tokens));
    let mut prev_time = Instant::now();

    let mut aho = build_ac(&tokens);

    let sample_iter = FlatRepeatResult::new(|| -> std::io::Result<_> {
        println!("Start decoding from start of file");
        Ok(SampleReader::new(
            BufReader::new(Decoder::new(File::open(&path)?)?),
            true,
        ))
    });

    for sample in sample_iter {
        let sample = sample??;
        samples_since_add += 1;

        let mut prev_token: Option<usize> = None;

        for x in aho.find_iter(&sample.text) {
            let curr_token = x.pattern();

            tokens_since_add += 1;

            if let Some(prev_token) = prev_token {
                // only combine tokens that are both or neither whitespace
                if is_whitespace[prev_token] == is_whitespace[curr_token] {
                    let count = &mut bigram_count[(prev_token, curr_token)];
                    *count = count.saturating_add(1);

                    if *count > top_count {
                        top_count = *count;
                        top_index = Some((prev_token, curr_token));
                    }
                }
            }
            prev_token = Some(curr_token);
        }

        if top_count >= count_threshold && samples_since_add >= samples_threshold {
            println!(
                "Adding new token after {} samples, {} tokens, {} count",
                samples_since_add, tokens_since_add, top_count
            );

            // add top token
            {
                let now = Instant::now();
                let (top_a, top_b) = top_index.unwrap();
                assert_eq!(is_whitespace[top_a], is_whitespace[top_b]);

                let new_token = [tokens[top_a].as_slice(), tokens[top_b].as_slice()].concat();
                println!(
                    "Adding token {}: {:?} {:?} with count {} after {:?}",
                    tokens.len(),
                    String::from_utf8_lossy(&new_token),
                    new_token,
                    top_count,
                    now - prev_time,
                );

                tokens.push(new_token);
                is_whitespace.push(is_whitespace[top_a]);

                bigram_count[(top_a, top_b)] = 0;
                prev_time = now;
            }

            // invalidate state
            tokens_since_add = 0;
            samples_since_add = 0;
            aho = build_ac(&tokens);

            top_count = 0; // will immediately be set when incrementing again
            top_index = None;

            // clip and decay counts to ensure old tokens go away over time
            // TODO can we do this lazily while incrementing?
            //    keep a last_seen index per bigram, when visiting decay as appropriate
            bigram_count
                .slice_mut(s![..tokens.len(), ..tokens.len()])
                .mapv_inplace(|c| {
                    let clipped = min(c, count_threshold);
                    let scaled = clipped as u32 * count_decay_numerator / count_decay_denominator;
                    scaled as Count
                });
        }

        if tokens.len() >= max_tokens {
            break;
        }
    }

    let mut vocab_writer = BufWriter::new(File::create(vocab_path)?);
    serde_json::to_writer(&mut vocab_writer, &tokens)?;
    vocab_writer.flush()?;

    println!("Final tokens:");
    for token in &tokens {
        if let Ok(token) = std::str::from_utf8(token) {
            println!("  {:?}", token);
        } else {
            println!("  {:?}", token);
        }
    }

    Ok(())
}

fn build_ac(tokens: &[Vec<u8>]) -> AhoCorasick {
    AhoCorasickBuilder::new()
        .match_kind(MatchKind::LeftmostLongest)
        .dfa(true)
        .build(tokens)
}

use std::cmp::{min, Reverse};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use itertools::Itertools;
use ndarray::{s, Array2, Zip};
use serde::Serialize;

use kt_core::batch::build_tokenizer;
use kt_core::iter::FlatRepeatResult;
use kt_core::sample::SampleReader;

#[derive(Debug, Parser, Serialize)]
struct Args {
    input: PathBuf,
    output: PathBuf,

    debug_path: Option<PathBuf>,

    #[clap(long, default_value_t = 1024)]
    max_tokens: usize,
    #[clap(long, default_value_t = 1024)]
    max_drops: usize,

    #[clap(long, default_value_t = 2.0)]
    threshold_drop_factor: f32,

    #[clap(long, default_value_t = 10000)]
    threshold_count: u32,
    #[clap(long, default_value_t = 100)]
    threshold_samples: u32,

    #[clap(long, default_value_t = 0.99)]
    count_decay: f32,
}

#[derive(Debug, Serialize)]
struct Output {
    args: Args,
    tokens: Vec<Vec<u8>>,
}

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
    let args: Args = Args::parse();
    println!("Args: {:#?}", args);

    assert_eq!("zst", args.input.extension().unwrap());
    assert_eq!("json", args.output.extension().unwrap());
    std::fs::create_dir_all(args.output.parent().unwrap())?;

    let max_tokens = args.max_tokens;
    let max_drops = args.max_drops;
    let threshold_drop_factor = args.threshold_drop_factor;
    let threshold_count = args.threshold_count;
    let threshold_samples = args.threshold_samples;
    assert!(threshold_count < Count::MAX);

    assert!((0.0..1.0).contains(&args.count_decay));
    let count_decay_numerator: u32 = (args.count_decay * 1000.0) as u32;
    let count_decay_denominator: u32 = 1000;

    // start with a token for each possible byte
    let mut tokens = (0..u8::MAX).map(|x| vec![x]).collect_vec();
    let mut is_whitespace = (0..u8::MAX)
        .map(|c| (c as char).is_whitespace())
        .collect_vec();
    let forced_token_count = u8::MAX as usize;

    let mut aho = build_tokenizer(&tokens);

    let mut bigram_count: Array2<Count> = Array2::zeros((max_tokens, max_tokens));
    let mut unigram_count: Vec<Count> = vec![0; tokens.len()];
    let mut has_been_merged: Vec<bool> = vec![false; tokens.len()];

    let mut tokens_since_add = 0;
    let mut samples_since_add = 0;
    let mut drops_applied = 0;
    let mut top_count = 0;
    let mut top_index = None;
    let mut prev_time = Instant::now();

    let mut dropped_tokens = vec![];

    let sample_iter = FlatRepeatResult::new(|| -> std::io::Result<_> {
        println!("Start decoding from start of file");
        Ok(SampleReader::new_decode(
            File::open(&args.input)?,
            true,
            true,
        )?)
    });

    for sample in sample_iter {
        let sample = sample??;
        samples_since_add += 1;

        let mut prev_token: Option<usize> = None;

        for x in aho.find_iter(&sample.text) {
            let curr_token = x.pattern();

            tokens_since_add += 1;
            unigram_count[curr_token] += 1;

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

        if top_count >= threshold_count && samples_since_add >= threshold_samples {
            println!(
                "Adding new token after {} samples, {} tokens, {} count",
                samples_since_add, tokens_since_add, top_count
            );

            let now = Instant::now();
            let (top_a, top_b) = top_index.unwrap();

            // add top token
            {
                assert_eq!(is_whitespace[top_a], is_whitespace[top_b]);

                let new_token = [tokens[top_a].as_slice(), tokens[top_b].as_slice()].concat();
                println!(
                    "  token {}: {:?} {:?} with count {} after {:?}",
                    tokens.len(),
                    String::from_utf8_lossy(&new_token),
                    new_token,
                    top_count,
                    now - prev_time,
                );

                tokens.push(new_token);
                is_whitespace.push(is_whitespace[top_a]);
                has_been_merged[top_a] |= true;
                has_been_merged[top_b] |= true;
                has_been_merged.push(false);

                bigram_count[(top_a, top_b)] = 0;
                prev_time = now;

                // inherit bigram count
                unigram_count.push(top_count);
                unigram_count[top_a] -= top_count;
                unigram_count[top_b] -= top_count;
            }

            // TODO non-greedy tokenization (eg. try to capture as many bytes as possible per 3 tokens instead of only 1)
            //   but then what about ping-pong stuff where the first token can constantly jump to a different short one?
            //   only do non-greedy search per-"word"? kind of lame
            // TODO immediately drop tokens with counts that reach zero
            // TODO have some variance estimate for token counts, and only remove tokens below eg. 2*sigma
            // TODO immediately add all tokens with counts > over threshold
            //   overlapping ones will decay and be removed later anyway
            // TODO decay per sample instead of per added token somehow?
            //   maybe just decay on access (and store last modification time)

            // find least used token that has been merged, skip forced & last token
            let least_used_token = unigram_count[forced_token_count..unigram_count.len() - 1]
                .iter()
                .enumerate()
                .map(|(i, &c)| (i + forced_token_count, c))
                .filter(|&(i, _)| has_been_merged[i])
                .min_by_key(|&(_, c)| c)
                .map(|(i, _)| i);

            if let Some(least_used_token) = least_used_token {
                let least_used_count = unigram_count[least_used_token];

                println!(
                    "Least used token: {:?} with count {} ({} drops)",
                    ByteString(&tokens[least_used_token]),
                    least_used_count,
                    drops_applied,
                );

                // possibly remove least-used token
                if least_used_count == 0
                    || (tokens.len() == max_tokens
                        && drops_applied < max_drops
                        && ((least_used_count as f32 * threshold_drop_factor) as Count) < top_count)
                {
                    // if it's the least used by a large enough margin and not the most recent token
                    drops_applied += 1;
                    println!(
                        "Removing token {:?} with only {} uses",
                        ByteString(&tokens[least_used_token]),
                        least_used_count
                    );

                    let a = least_used_token;
                    let b = tokens.len() - 1;

                    // remove by swapping everything
                    dropped_tokens.push((tokens.swap_remove(a), least_used_count));
                    unigram_count.swap_remove(a);
                    has_been_merged.swap_remove(a);

                    // 2D array, one swap operation per axis
                    let (a_slice, b_slice) = bigram_count.multi_slice_mut((s![a, ..], s![b, ..]));
                    Zip::from(a_slice).and(b_slice).for_each(std::mem::swap);
                    let (a_slice, b_slice) = bigram_count.multi_slice_mut((s![.., a], s![.., b]));
                    Zip::from(a_slice).and(b_slice).for_each(std::mem::swap);
                }
            }

            // invalidate state
            tokens_since_add = 0;
            samples_since_add = 0;
            aho = build_tokenizer(&tokens);

            top_count = 0; // will immediately be set when incrementing again
            top_index = None;

            // clip and decay counts to ensure old tokens go away over time
            // TODO can we do this lazily while incrementing?
            //    keep a last_seen index per bigram, when visiting decay as appropriate
            bigram_count
                .slice_mut(s![..tokens.len(), ..tokens.len()])
                .mapv_inplace(|c| {
                    let clipped = min(c, threshold_count);
                    let scaled = clipped as u32 * count_decay_numerator / count_decay_denominator;
                    scaled as Count
                });

            unigram_count.iter_mut().for_each(|c| {
                *c = (*c as u32 * count_decay_numerator / count_decay_denominator) as Count
            });
        }

        if tokens.len() >= max_tokens {
            break;
        }
    }

    if let Some(debug_path) = &args.debug_path {
        let mut debug_writer = BufWriter::new(File::create(debug_path)?);

        let mut indices = (0..tokens.len()).collect_vec();
        indices.sort_by_key(|&i| Reverse(unigram_count[i]));

        writeln!(&mut debug_writer, "Token: (token: count forced)")?;
        for i in indices {
            writeln!(
                &mut debug_writer,
                "  {:?}: {} {}",
                ByteString(&tokens[i]),
                unigram_count[i],
                i < forced_token_count,
            )?;
        }

        writeln!(&mut debug_writer, "\n\nDropped tokens:")?;
        for (token, count) in dropped_tokens {
            writeln!(&mut debug_writer, "  {:?}: {}", ByteString(&token), count)?;
        }

        debug_writer.flush()?;
        drop(debug_writer);
    }

    println!("Writing output file");
    let mut vocab_writer = BufWriter::new(File::create(&args.output)?);
    let output = Output { args, tokens };
    serde_json::to_writer(&mut vocab_writer, &output)?;
    vocab_writer.flush()?;

    Ok(())
}

struct ByteString<'a>(&'a [u8]);

impl<'a> Debug for ByteString<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(self.0) {
            write!(f, "{:?}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

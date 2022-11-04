use std::cmp::min;
use std::collections::VecDeque;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use ndarray::Array2;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

pub struct Batcher {
    // settings
    batch_size: usize,
    seq_len: usize,
    bucket_count: usize,
    aho: AhoCorasick,

    // state
    rng: SmallRng,
    stats: Stats,
    buckets: VecDeque<Bucket>,
    empty_buffers: VecDeque<VecDeque<usize>>,
}

#[derive(Debug, Default, Copy, Clone)]
pub struct Stats {
    pub sample_count: usize,
    pub token_count: usize,
    pub batch_count: usize,
}

pub struct Batch {
    pub tokens: Array2<i32>,
    pub samples: Vec<usize>,
    pub start_indices: Vec<usize>,
}

pub struct Bucket {
    // which sample index this data originated from
    sample: usize,
    // which token is currently at the front
    start_index: usize,

    // random number of tokens at the start has already been dropped
    tokens: VecDeque<usize>,
}

pub fn build_tokenizer<I, P>(tokens: I) -> AhoCorasick
where
    I: IntoIterator<Item = P>,
    P: AsRef<[u8]>,
{
    AhoCorasickBuilder::new()
        .match_kind(MatchKind::LeftmostLongest)
        .dfa(true)
        .build(tokens)
}

impl Batcher {
    pub fn new<I, P>(batch_size: usize, seq_len: usize, bucket_count: usize, tokens: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        Self {
            batch_size,
            seq_len,
            bucket_count,
            aho: build_tokenizer(tokens),
            rng: SmallRng::from_entropy(),
            stats: Stats::default(),
            buckets: VecDeque::default(),
            empty_buffers: VecDeque::default(),
        }
    }

    pub fn push_sample(&mut self, sample: &str) -> bool {
        // don't even bother with empty sequences, they would create empty buckets
        if sample.is_empty() {
            return false;
        }

        // pick a buffer to put the sequences into
        let mut buffer = self
            .empty_buffers
            .pop_front()
            .unwrap_or_else(VecDeque::default);
        assert!(buffer.is_empty(), "Empty buffer is non-empty");

        // tokenize straight into buffer
        buffer.extend(self.aho.find_iter(sample).map(|m| m.pattern()));
        assert!(!buffer.is_empty(), "Newly filled buffer is empty",);
        let parsed_token_count = buffer.len();

        // drop random tokens at the start
        let offset = if buffer.len() > self.seq_len {
            self.rng.gen_range(0..self.seq_len)
        } else {
            0
        };
        drop(buffer.drain(0..offset));

        // save bucket
        let bucket = Bucket {
            sample: self.stats.sample_count,
            start_index: offset,
            tokens: buffer,
        };
        self.buckets.push_back(bucket);

        // update stats
        self.stats.sample_count += 1;
        self.stats.token_count += parsed_token_count;

        true
    }

    pub fn pop_batch(&mut self) -> Option<Batch> {
        if self.buckets.len() < self.bucket_count {
            return None;
        }

        let mut batch: Array2<i32> = Array2::zeros((self.batch_size, self.seq_len));
        batch.fill(-1);
        let mut samples = vec![];
        let mut start_indices = vec![];

        for bi in 0..self.batch_size {
            // TODO we might sample multiple times from the same bucket, is that a problem?
            // pick a random non-empty bucket
            let bucket_index = self.rng.gen_range(0..self.buckets.len());
            let bucket = &mut self.buckets[bucket_index];
            samples.push(bucket.sample);
            start_indices.push(bucket.start_index);

            // we can initially get less tokens than seq_len, that just means the sample was short, keep it
            let curr_seq_len = min(self.seq_len, bucket.tokens.len());
            assert!(curr_seq_len > 0, "Non-empty bucket is empty");

            // copy tokens into batch (and remove from buffer)
            let drain = bucket.tokens.drain(0..curr_seq_len);
            for (i, token) in drain.enumerate() {
                batch[(bi, i)] = token as i32;
            }
            bucket.start_index += curr_seq_len;

            // remove potentially empty buffer
            // if we have less than seq_len tokens left at this point they're just overflow, drop them
            //   (they could have been sampled because of the offset, we're not introducing bias here)
            if bucket.tokens.len() < self.seq_len {
                bucket.tokens.clear();
                self.empty_buffers
                    .push_back(self.buckets.remove(bucket_index).unwrap().tokens);
            }
        }

        let batch = Batch {
            tokens: batch,
            samples,
            start_indices,
        };
        self.stats.batch_count += 1;
        Some(batch)
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }
}

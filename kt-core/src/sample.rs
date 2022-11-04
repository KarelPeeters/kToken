use std::io::{BufRead, BufReader, Read};

use serde::Deserialize;
use zstd::Decoder;

use crate::unicode::str_is_ltr;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Sample {
    pub text: String,
    pub meta: Meta,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Meta {
    pub pile_set_name: String,
}

pub struct SampleReader<R: BufRead> {
    reader: R,
    line: String,
    remove_rtl: bool,
}

impl<R: Read> SampleReader<BufReader<Decoder<'static, BufReader<R>>>> {
    pub fn new_decode(reader: R, remove_rtl: bool) -> std::io::Result<Self> {
        Ok(Self::new(BufReader::new(Decoder::new(reader)?), remove_rtl))
    }
}

impl<R: BufRead> SampleReader<R> {
    pub fn new(reader: R, remove_rtl: bool) -> Self {
        Self {
            reader,
            line: String::new(),
            remove_rtl,
        }
    }

    fn next_io(&mut self) -> std::io::Result<Option<Sample>> {
        loop {
            self.line.clear();
            self.reader.read_line(&mut self.line)?;

            if self.line.is_empty() {
                // EOF reached
                return Ok(None);
            }

            let sample: Sample = serde_json::from_str(&self.line)?;

            if self.remove_rtl && !str_is_ltr(&sample.text) {
                // skip RTL text
                continue;
            }

            return Ok(Some(sample));
        }
    }
}

impl<R: BufRead> Iterator for SampleReader<R> {
    type Item = std::io::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_io().transpose()
    }
}

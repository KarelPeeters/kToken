use std::io::BufRead;

use serde::Deserialize;

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
}

impl<R: BufRead> SampleReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line: String::new(),
        }
    }

    fn next_io(&mut self) -> std::io::Result<Option<Sample>> {
        self.line.clear();
        self.reader.read_line(&mut self.line)?;

        if self.line.is_empty() {
            // EOF reached
            return Ok(None);
        }

        let sample: Sample = serde_json::from_str(&self.line)?;
        Ok(Some(sample))
    }
}

impl<R: BufRead> Iterator for SampleReader<R> {
    type Item = std::io::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_io().transpose()
    }
}

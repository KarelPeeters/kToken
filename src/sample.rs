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

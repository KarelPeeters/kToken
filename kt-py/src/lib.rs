use std::fs::File;
use std::io::ErrorKind;
use std::path::PathBuf;

use aho_corasick::AhoCorasick;
use flume::{Receiver, RecvError, SendError, Sender};
use itertools::Itertools;
use numpy::IntoPyArray;
use numpy::{PyArray1, PyArray2};
use pyo3::prelude::*;

use kt_core::batch::{build_tokenizer, Batch, Batcher};
use kt_core::sample::SampleReader;

#[pymodule]
fn ktoken(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Tokenizer>()?;
    m.add_class::<BatchTokenReader>()?;
    Ok(())
}

#[pyclass]
struct BatchTokenReader {
    receiver: Receiver<Message>,
}

#[pyclass]
struct Tokenizer {
    aho: AhoCorasick,
}

#[pymethods]
impl Tokenizer {
    #[new]
    fn new(tokens: Vec<Vec<u8>>) -> Self {
        Tokenizer {
            aho: build_tokenizer(&tokens),
        }
    }

    fn tokenize<'py>(&self, py: Python<'py>, s: &str) -> &'py PyArray1<i32> {
        self.aho
            .find_iter(s)
            .map(|m| m.pattern() as i32)
            .collect_vec()
            .into_pyarray(py)
    }
}

#[pymethods]
impl BatchTokenReader {
    #[new]
    fn new(
        tokens: Vec<Vec<u8>>,
        data_paths: Vec<PathBuf>,
        batch_size: usize,
        seq_len: usize,
        bucket_count: usize,
        queue_size: usize,
    ) -> PyResult<Self> {
        for path in &data_paths {
            if !path.exists() {
                return Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("Data path {:?} does not exist", path),
                )
                .into());
            }
        }

        let batcher = Batcher::new(batch_size, seq_len, bucket_count, tokens);
        let (sender, receiver) = flume::bounded(queue_size);

        std::thread::Builder::new()
            .name(String::from("BatchTokenReader"))
            .spawn(move || batcher_thread_main(batcher, sender, data_paths))?;

        Ok(BatchTokenReader { receiver })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<&'py PyArray2<i32>>> {
        let batch = match self.receiver.recv() {
            Ok(Message::Batch(batch)) => batch,
            Ok(Message::Error(err)) => return Err(err.into()),
            Err(RecvError::Disconnected) => return Ok(None),
        };

        Ok(Some(PyArray2::from_owned_array(py, batch.tokens)))
    }
}

enum Message {
    Batch(Batch),
    Error(std::io::Error),
}

fn batcher_thread_main(batcher: Batcher, sender: Sender<Message>, data_paths: Vec<PathBuf>) {
    match batcher_thread_main_inner(batcher, &sender, data_paths) {
        Ok(()) => {}
        Err(err) => {
            // ignore errors caused by the sender, we're already closing everything anyway
            let _ = sender.send(Message::Error(err));
        }
    };

    // drop & close sender
    drop(sender);
}

fn batcher_thread_main_inner(
    mut batcher: Batcher,
    sender: &Sender<Message>,
    data_paths: Vec<PathBuf>,
) -> std::io::Result<()> {
    loop {
        let mut all_empty = true;

        for path in &data_paths {
            let file = File::open(path)?;
            for sample in SampleReader::new_decode(file, true, true)? {
                let sample = sample?;

                if batcher.push_sample(&sample.text) {
                    all_empty = false;
                }

                while let Some(batch) = batcher.pop_batch() {
                    match sender.send(Message::Batch(batch)) {
                        Ok(()) => {}
                        // receiver got closed, we can stop as well
                        Err(SendError(_)) => break,
                    }
                }
            }
        }

        // none of the files (if any) contain a sample, break infinite loop
        if all_empty {
            break;
        }
    }

    Ok(())
}

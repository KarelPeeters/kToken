use pyo3::prelude::*;

#[pyfunction]
fn foo(x: i64) -> i64 {
    -x
}

/// A Python module implemented in Rust.
#[pymodule]
fn ktoken(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(foo, m)?)?;
    Ok(())
}

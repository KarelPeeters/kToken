pub struct FlatRepeatResult<I: Iterator, E, F: Fn() -> Result<I, E>> {
    left: Option<I>,
    f: F,
}

impl<I: Iterator, E, F: Fn() -> Result<I, E>> FlatRepeatResult<I, E, F> {
    pub fn new(f: F) -> Self {
        FlatRepeatResult { left: None, f }
    }
}

impl<I: Iterator, E, F: Fn() -> Result<I, E>> Iterator for FlatRepeatResult<I, E, F> {
    type Item = Result<I::Item, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(left) = &mut self.left {
                if let Some(item) = left.next() {
                    return Some(Ok(item));
                }
            }

            match (self.f)() {
                Ok(left) => self.left = Some(left),
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

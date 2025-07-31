use std::{
    io::{stdout, Write},
    fmt,
};

pub enum ProgChar {
    Block,
    Hash,
}

impl fmt::Display for ProgChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let c = match self {
            ProgChar::Block => '█',
            ProgChar::Hash => '#', };
        write!(f, "{c}")
    }
}

/// A progress bar wrapper for iterators, similar to Python's tqdm
#[allow(non_camel_case_types)]
pub struct tqdm<T: Iterator> {
    iter: T,
    total: usize,
    current: usize,
    width: usize,
    prog_char: ProgChar,
    message: Option<String>,
}

impl<T: Iterator> tqdm<T> {
    /// Creates a new tqdm progress bar with an optional message (max 50 chars)
    pub fn new(iter: T, total: usize, prog_char: Option<ProgChar>, message: Option<&str>) -> Self {
        let message = message.map(|s| s.chars().take(50).collect());
        tqdm {
            iter,
            total,
            current: 0,
            width: 50,
            prog_char: prog_char.unwrap_or(ProgChar::Hash),
            message,
        }
    }

    /// Renders the progress bar with optional message to stdout
    fn render(&self) {
        let progress = self.current as f64 / self.total as f64;
        let filled = (progress * self.width as f64) as usize;
        let empty = self.width - filled;

        print!("\r[");
        for _ in 0..filled {
            print!("{0}", self.prog_char);
        }
        for _ in 0..empty {
            print!("-");
        }
        print!("] {:.1}%", progress * 100.0);
        if let Some(ref msg) = self.message {
            print!(" {msg}");
        }
        stdout().flush().unwrap();
    }
}

impl<T: Iterator> Iterator for tqdm<T> {
    type Item = T::Item;

    /// Advances the iterator and updates the progress bar
    fn next(&mut self) -> Option<Self::Item> {
        self.current += 1;
        self.render();
        self.iter.next()
    }
}

impl<T: Iterator> Drop for tqdm<T> {
    /// Ensures a newline is printed when tqdm is dropped to prevent overwriting
    fn drop(&mut self) {
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests creation of tqdm with correct initialization
    #[test]
    fn test_tqdm_creation() {
        let iter = 0..100;
        let tqdm = tqdm::new(iter, 100, None, None);
        assert_eq!(tqdm.total, 100);
        assert_eq!(tqdm.current, 0);
        assert_eq!(tqdm.width, 50);
    }

    /// iteration increments current correctly
    #[test]
    fn test_tqdm_iteration() {
        let iter = 0..3;
        let mut tqdm = tqdm::new(iter, 3, None, None);
        assert_eq!(tqdm.next(), Some(0));
        assert_eq!(tqdm.current, 1);
        assert_eq!(tqdm.next(), Some(1));
        assert_eq!(tqdm.current, 2);
        assert_eq!(tqdm.next(), Some(2));
        assert_eq!(tqdm.current, 3);
        assert_eq!(tqdm.next(), None);
    }

    /// completes iteration correctly
    #[test]
    fn test_tqdm_complete() {
        let iter = 0..5;
        let mut tqdm = tqdm::new(iter, 5, None, None);
        let mut count = 0;
        while tqdm.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
        assert_eq!(tqdm.current, 5);
    }

    /// with empty iterator
    #[test]
    fn test_tqdm_empty() {
        let iter: Vec<u32> = vec![];
        let mut tqdm = tqdm::new(iter.into_iter(), 0, None, None);
        assert_eq!(tqdm.next(), None);
        assert_eq!(tqdm.current, 1);
    }
}


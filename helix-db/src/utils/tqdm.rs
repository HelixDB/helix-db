use std::{
    fmt,
    io::{Write, stdout},
};

pub enum ProgChar {
    Block,
    Hash,
}

impl fmt::Display for ProgChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let c = match self {
            ProgChar::Block => '█',
            ProgChar::Hash => '#',
        };
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

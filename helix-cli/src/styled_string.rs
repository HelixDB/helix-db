#[allow(dead_code)]
pub trait StyledString {
    fn black(&self) -> String;
    fn red(&self) -> String;
    fn green(&self) -> String;
    fn yellow(&self) -> String;
    fn blue(&self) -> String;
    fn magenta(&self) -> String;
    fn cyan(&self) -> String;
    fn white(&self) -> String;
    fn bold(&self) -> String;
    fn underline(&self) -> String;
}

impl StyledString for str {
    fn black(&self) -> String {
        format!("\x1b[30m{}\x1b[0m", self)
    }

    fn red(&self) -> String {
        format!("\x1b[31m{}\x1b[0m", self)
    }

    fn green(&self) -> String {
        format!("\x1b[32m{}\x1b[0m", self)
    }

    fn yellow(&self) -> String {
        format!("\x1b[33m{}\x1b[0m", self)
    }

    fn blue(&self) -> String {
        format!("\x1b[34m{}\x1b[0m", self)
    }

    fn magenta(&self) -> String {
        format!("\x1b[35m{}\x1b[0m", self)
    }

    fn cyan(&self) -> String {
        format!("\x1b[36m{}\x1b[0m", self)
    }

    fn white(&self) -> String {
        format!("\x1b[37m{}\x1b[0m", self)
    }

    fn bold(&self) -> String {
        format!("\x1b[1m{}\x1b[0m", self)
    }

    fn underline(&self) -> String {
        format!("\x1b[4m{}\x1b[0m", self)
    }
}

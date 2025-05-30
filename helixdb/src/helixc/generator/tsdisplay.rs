use std::io::{self, Write};

pub trait ToTypeScript {
    fn to_typescript(&self) -> String;
}

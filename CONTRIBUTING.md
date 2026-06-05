# Contributing to helix-db

Thank you for contributing to helix-db!

## Development Setup

1. **Requirements**
   - Rust 1.70+
   - Cargo

2. **Clone the repository**
   ```bash
   git clone https://github.com/HelixDB/helix-db.git
   cd helix-db
   ```

3. **Build the project**
   ```bash
   cargo build --release
   ```

4. **Run tests**
   ```bash
   cargo test
   cargo clippy  # Linting
   cargo fmt     # Formatting
   ```

## Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feat/your-feature-name
   ```

2. **Code style**
   - Run `cargo fmt` before committing
   - Run `cargo clippy` to catch common mistakes
   - Write idiomatic Rust

3. **Commit and push**
   ```bash
   git commit -m "feat: add your feature"
   git push origin feat/your-feature-name
   ```

## Pull Request Process

1. Fork the repository
2. Create your feature branch
3. Make your changes with tests
4. Ensure clippy and fmt pass
5. Submit a PR with description

## Reporting Issues

- Use GitHub Issues for bugs and feature requests
- Include Rust version and OS
- Provide reproduction steps

## License

By contributing, you agree that your contributions will be licensed under the project license.

# Contributing to FeOx Server

Thank you for your interest in contributing to FeOx Server! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## How to Contribute

### 1. Fork the Repository
- Fork the repository to your GitHub account
- Clone your fork locally
```bash
git clone https://github.com/mehrantsi/feox-server
cd feox-server
```

### 2. Create a Branch
- Create a branch for your changes
- Use a clear and descriptive branch name
```bash
git checkout -b feature/add-xxx-command
# or
git checkout -b fix/connection-handling
```

### 3. Make Your Changes
- Write clear, commented code following Rust best practices
- Follow the existing code style and formatting
- Add tests for new features
- Update documentation as needed

### 4. Code Style Guidelines
- Run `cargo fmt` before committing
- Run `cargo clippy -- -D warnings` to check for common issues
- Use meaningful variable and function names
- Add inline documentation for complex logic
- Keep functions focused and small

### 5. Test Your Changes
```bash
# Build the project
cargo build --release

# Run with test commands
./target/release/feox-server

# In another terminal, test with redis-cli
redis-cli ping
redis-cli set key value
redis-cli get key

# Run benchmarks
redis-benchmark -t SET,GET -n 100000
```

### 6. Commit Your Changes
- Use clear and meaningful commit messages
- Keep commits focused and atomic
- Reference any relevant issues
```bash
git add .
git commit -m "feat: add APPEND command support

- Implement APPEND command handler
- Add tests for edge cases
- Update README with new command

Fixes #123"
```

### 7. Submit a Pull Request
- Push your changes to your fork
- Submit a pull request to the main repository
- Provide a clear description of your changes
- Link any related issues

## Areas for Contribution

### High Priority
- **Additional Redis Commands**: Implementing missing Redis commands (Lists, Sets, Hashes)
- **Authentication**: Adding AUTH command and password protection
- **Persistence Improvements**: Optimizing write-ahead logging
- **TLS Support**: Adding secure connection support

### Good First Issues
- **Command Improvements**: Enhancing existing command implementations
- **Documentation**: Improving README, adding examples
- **Tests**: Adding tests
- **Benchmarks**: Creating benchmark suites

### Performance
- **Optimization**: Finding and fixing performance bottlenecks
- **Memory Usage**: Reducing memory footprint
- **Latency**: Improving response times

## Pull Request Guidelines

- **One feature per PR**: Keep pull requests focused on a single change
- **Update documentation**: Include README updates for new features
- **Add tests**: New features should include tests
- **Pass CI**: Ensure all checks pass
- **Clean history**: Squash commits if needed for clarity

## Development Setup

### Requirements
- Rust 1.70+
- Redis CLI (for testing)
- redis-benchmark (for performance testing)

### Building
```bash
# Debug build
cargo build

# Release build (recommended for testing)
cargo build --release
```

### Testing New Commands
When adding a new Redis command:
1. Add the command variant to `src/protocol/command/mod.rs`
2. Add parsing logic to `src/protocol/command/parser.rs`
3. Add execution logic to `src/protocol/command/executor.rs`
4. Test with redis-cli

## Reporting Issues

When reporting issues, please include:
- FeOx Server version
- Operating system and version
- Steps to reproduce
- Expected vs actual behavior
- Any error messages or logs

## Questions?

Feel free to open an issue for:
- Questions about the codebase
- Design discussions
- Feature proposals
- Implementation guidance

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
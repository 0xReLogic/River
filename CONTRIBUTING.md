# Contributing to River

Thank you for your interest in contributing to River! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Workflow](#development-workflow)
4. [Pull Request Process](#pull-request-process)
5. [Coding Standards](#coding-standards)
6. [Testing](#testing)
7. [Documentation](#documentation)
8. [Issue Reporting](#issue-reporting)

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) to foster an open and welcoming environment.

## Getting Started

### Prerequisites

- Go 1.18+
- Make (optional, for using the Makefile)

### Setting Up the Development Environment

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/river.git
   cd river
   ```
3. Add the upstream repository as a remote:
   ```bash
   git remote add upstream https://github.com/0xReLogic/river.git
   ```
4. Build the project:
   ```bash
   make build
   ```

## Development Workflow

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```
   or
   ```bash
   git checkout -b fix/your-bugfix-name
   ```

2. Make your changes and commit them with a descriptive commit message:
   ```bash
   git commit -m "Add feature: your feature description"
   ```

3. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

4. Create a pull request from your branch to the upstream `main` branch

## Pull Request Process

1. Ensure your code passes all tests and linting checks
2. Update the documentation to reflect any changes
3. Add or update tests to cover your changes
4. Make sure your commit history is clean and meaningful
5. Submit your pull request with a clear description of the changes

## Coding Standards

We follow the standard Go coding conventions:

- Use `gofmt` to format your code
- Follow the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Run `golangci-lint` to check for common issues:
  ```bash
  make lint
  ```

## Testing

All new features and bugfixes should include tests. To run the tests:

```bash
make test
```

To run benchmarks:

```bash
make bench
```

## Documentation

Please update the documentation when adding or modifying features:

- Update the README.md if necessary
- Add or update documentation in the docs/ directory
- Include code comments for exported functions and types

## Issue Reporting

If you find a bug or have a feature request, please create an issue on GitHub:

1. Check if the issue already exists
2. Use the issue template to create a new issue
3. Provide as much detail as possible, including:
   - Steps to reproduce (for bugs)
   - Expected behavior
   - Actual behavior
   - Environment details (OS, Go version, etc.)

## Performance Improvements

When submitting performance improvements:

1. Include benchmark results before and after your changes
2. Explain the approach you took to improve performance
3. Consider the trade-offs of your approach (memory usage, complexity, etc.)

## Security Issues

If you discover a security vulnerability, please do NOT open an issue. Email [security@example.com](mailto:security@example.com) instead.

## License

By contributing to River, you agree that your contributions will be licensed under the project's [MIT License](LICENSE).
# 🐹 go-polars ⚡

![Intro](assets/images/go-rust.png)

🚀 This project creates Go bindings for the blazing-fast Polars data manipulation library!

## 🤔 What is Polars?
Polars is an open-source library for data manipulation, known for being one of the fastest data processing solutions on a single machine. It features a well-structured, typed API that is both expressive and easy to use.

https://github.com/pola-rs/polars

## 📦 Installation

### Prerequisites

- **Rust**: Install from [rustup.rs](https://rustup.rs/) or run:
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **Build tools**:
  ```bash
  sudo apt-get install build-essential  # Ubuntu/Debian
  # or
  sudo dnf install make automake gcc gcc-c++ kernel-devel   # CentOS/RHEL
  ```

### Quick Start

```bash
# Clone and build the library
git clone https://github.com/jordandelbar/go-polars
cd go-polars

# Build automatically (detects your OS)
./build.sh

# Or use make
make local-build

# Run examples
make run-basic-example
```

### Why Build Locally?
We **don't include pre-compiled binaries** in the repository because:
- 🔒 **Security**: You build from source you can trust
- 🖥️ **Platform support**: Works on Linux, macOS, and Windows
- 📦 **Smaller repo**: No 40MB+ binary files
- 🔄 **Always up-to-date**: Latest optimizations for your system

## ✨ Features

### Expression Operations

go-polars supports a comprehensive set of expression operations for data manipulation:

#### Comparison Operations
- `Gt(value)` - Greater than
- `Lt(value)` - Less than
- `Eq(value)` - Equal to
- `Ne(value)` - Not equal to
- `Ge(value)` - Greater than or equal to
- `Le(value)` - Less than or equal to

#### Mathematical Operations
- `Add(expr)` / `AddValue(value)` - Addition
- `Sub(expr)` / `SubValue(value)` - Subtraction
- `Mul(expr)` / `MulValue(value)` - Multiplication
- `Div(expr)` / `DivValue(value)` - Division

#### Logical Operations
- `And(expr)` - Logical AND
- `Or(expr)` - Logical OR
- `Not()` - Logical NOT

#### Basic Usage Examples

```go
import "github.com/jordandelbar/go-polars/polars"

// Load data
df, err := polars.ReadCSV("data.csv")

// Comparison operations
filtered := df.Filter(polars.Col("age").Gt(25))          // Adults only
equals := df.Filter(polars.Col("score").Eq(100))         // Perfect scores

// Mathematical operations
df = df.WithColumns(
    polars.Col("price").MulValue(1.1).Alias("price_with_tax"),  // Add 10% tax
    polars.Col("length").Add(polars.Col("width")).Alias("perimeter"), // Calculate perimeter
)

// Logical operations
complex := df.Filter(
    polars.Col("age").Gt(18).And(polars.Col("score").Ge(80)),  // Adults with good scores
)

// Chaining operations
result := df.
    Filter(polars.Col("active").Eq(1)).                         // Active users only
    WithColumns(polars.Col("salary").MulValue(1.05).Alias("new_salary")). // 5% raise
    Select(polars.Col("name"), polars.Col("new_salary"))        // Select relevant columns
```

## 🚀 Examples & Quick Start

### Basic Example
Get started with simple DataFrame operations:
```bash
make run-basic-example
```

### Expression Operations Demo
Try out all the new comparison, mathematical, and logical operations:
```bash
cd examples/expressions && go run simple_example.go
```

### Comprehensive Demo
Run the full-featured example with complex operations:
```bash
make run-expressions-demo
```

### Available Make Commands
- `make local-build` - Build the library from source
- `make run-basic-example` - Run basic DataFrame demo
- `make run-expressions-example` - Run expression operations demo
- `make run-expressions-demo` - Run comprehensive feature demo

## 🧪 Testing

This project includes comprehensive test coverage:

- **📊 DataFrame Operations**: Basic operations, filtering, selection
- **🔧 Expression Operations**: All comparison, mathematical, and logical operations
- **💾 I/O Operations**: CSV and Parquet read/write with error handling
- **🔗 Complex Scenarios**: Chained operations, edge cases, memory management
- **⚡ Performance**: Benchmarks for critical operations

```bash
# Run all tests
make test

# Quick test run
make test-short

# Performance benchmarks
make test-bench

# Generate coverage report
make test-coverage
```

## 🤝 Contributing

1. **Fork** the repository
2. **Build** locally: `./build.sh`
3. **Test** your changes: `make test`
4. **Submit** a pull request

## 📄 License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

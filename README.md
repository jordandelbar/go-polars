# 🐹 go-polars ⚡

![Intro](assets/images/go-rust.png)

🚀 This project creates Go bindings for the blazing-fast Polars data manipulation library!

## 🤔 What is Polars?
Polars is an open-source library for data manipulation, known for being one of the fastest data processing solutions on a single machine. It features a well-structured, typed API that is both expressive and easy to use. 📈

https://github.com/pola-rs/polars

## 📦 Installation
```bash
make -v
```

```bash
sudo apt-get install build-essential
```

```bash
make local-build
make run-basic-example
```

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

// ➕ Mathematical operations
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
- `make local-build` - 🔨 Build the library from source
- `make run-basic-example` - 🏃‍♂️ Run basic DataFrame demo
- `make run-expressions-example` - 🎯 Run expression operations demo
- `make run-expressions-demo` - 🧪 Run comprehensive feature demo

## 📄 License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Load the iris dataset
	irisDf, err := polars.ReadCSV("../../testdata/iris.csv")
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Original Iris DataFrame ===")
	fmt.Println(irisDf.Head(5))

	fmt.Println("\n=== Comparison Operations ===")

	// Greater than
	gtFilter := irisDf.Filter(polars.Col("petal.length").Gt(5))
	fmt.Printf("Rows where petal.length > 5: %d rows\n", gtFilter.Height())

	// Less than
	ltFilter := irisDf.Filter(polars.Col("petal.length").Lt(2))
	fmt.Printf("Rows where petal.length < 2: %d rows\n", ltFilter.Height())

	// Equal to
	eqFilter := irisDf.Filter(polars.Col("sepal.length").Eq(5))
	fmt.Printf("Rows where sepal.length == 5: %d rows\n", eqFilter.Height())

	// Not equal to
	neFilter := irisDf.Filter(polars.Col("sepal.length").Ne(5))
	fmt.Printf("Rows where sepal.length != 5: %d rows\n", neFilter.Height())

	// Greater than or equal to
	geFilter := irisDf.Filter(polars.Col("petal.length").Ge(5))
	fmt.Printf("Rows where petal.length >= 5: %d rows\n", geFilter.Height())

	// Less than or equal to
	leFilter := irisDf.Filter(polars.Col("petal.length").Le(2))
	fmt.Printf("Rows where petal.length <= 2: %d rows\n", leFilter.Height())

	fmt.Println("\n=== Mathematical Operations ===")

	// Add value to column
	mathDf := irisDf.WithColumns(
		polars.Col("petal.length").AddValue(1.0).Alias("petal_length_plus_1"),
		polars.Col("petal.width").MulValue(2.0).Alias("petal_width_doubled"),
		polars.Col("sepal.length").SubValue(0.5).Alias("sepal_length_minus_half"),
		polars.Col("sepal.width").DivValue(2.0).Alias("sepal_width_halved"),
	)
	fmt.Println("DataFrame with mathematical operations:")
	fmt.Println(mathDf.Select(
		polars.Col("petal.length"),
		polars.Col("petal_length_plus_1"),
		polars.Col("petal.width"),
		polars.Col("petal_width_doubled"),
	).Head(5))

	fmt.Println("\n=== Column-to-Column Mathematical Operations ===")

	// Add two columns together
	columnMathDf := irisDf.WithColumns(
		polars.Col("petal.length").Add(polars.Col("petal.width")).Alias("petal_sum"),
		polars.Col("sepal.length").Sub(polars.Col("sepal.width")).Alias("sepal_diff"),
		polars.Col("petal.length").Mul(polars.Col("petal.width")).Alias("petal_area"),
		polars.Col("sepal.length").Div(polars.Col("sepal.width")).Alias("sepal_ratio"),
	)

	fmt.Println("DataFrame with column-to-column operations:")
	fmt.Println(columnMathDf.Select(
		polars.Col("petal.length"),
		polars.Col("petal.width"),
		polars.Col("petal_sum"),
		polars.Col("petal_area"),
	).Head(5))

	fmt.Println("\n=== Logical Operations ===")

	// Combine conditions with AND
	andFilter := irisDf.Filter(
		polars.Col("petal.length").Gt(4).And(polars.Col("petal.width").Gt(1)),
	)
	fmt.Printf("Rows where petal.length > 4 AND petal.width > 1: %d rows\n", andFilter.Height())

	// Combine conditions with OR
	orFilter := irisDf.Filter(
		polars.Col("petal.length").Lt(2).Or(polars.Col("petal.width").Gt(2)),
	)
	fmt.Printf("Rows where petal.length < 2 OR petal.width > 2: %d rows\n", orFilter.Height())

	// NOT operation
	notFilter := irisDf.Filter(
		polars.Col("petal.length").Gt(4).Not(),
	)
	fmt.Printf("Rows where NOT(petal.length > 4): %d rows\n", notFilter.Height())

	fmt.Println("\n=== Complex Expression Combinations ===")

	// Complex filtering: (petal.length > 3 AND petal.width > 1) OR (sepal.length < 5)
	complexFilter := irisDf.Filter(
		polars.Col("petal.length").Gt(3).And(polars.Col("petal.width").Gt(1)).Or(
			polars.Col("sepal.length").Lt(5),
		),
	)
	fmt.Printf("Complex filter result: %d rows\n", complexFilter.Height())
	fmt.Println("Sample of complex filtered data:")
	fmt.Println(complexFilter.Head(3))

	// Create a new column with complex mathematical expression
	complexMathDf := irisDf.WithColumns(
		polars.Col("petal.length").MulValue(2.0).Add(polars.Col("petal.width")).SubValue(1.0).Alias("complex_calc"),
	)

	fmt.Println("\n=== Complex Mathematical Expression ===")
	fmt.Println("Formula: (petal.length * 2) + petal.width - 1")
	fmt.Println(complexMathDf.Select(
		polars.Col("petal.length"),
		polars.Col("petal.width"),
		polars.Col("complex_calc"),
	).Head(5))

	fmt.Println("\n=== Chained Operations Example ===")

	// Chain multiple operations together
	chainedDf := irisDf.
		Filter(polars.Col("petal.length").Gt(1)).                    // Filter first
		WithColumns(polars.Col("petal.length").MulValue(10.0).Alias("petal_length_mm")). // Convert to mm
		Filter(polars.Col("petal_length_mm").Le(50)).                // Filter again
		Select(polars.Col("variety"), polars.Col("petal.length"), polars.Col("petal_length_mm"))

	fmt.Printf("Chained operations result: %d rows\n", chainedDf.Height())
	fmt.Println(chainedDf.Head(5))

	// Save some results
	err = complexFilter.WriteCSV("complex_filtered_iris.csv")
	if err != nil {
		log.Printf("Error saving CSV: %v", err)
	} else {
		fmt.Println("\nComplex filtered results saved to 'complex_filtered_iris.csv'")
	}

	fmt.Println("\n=== Expression Operations Demo Complete! ===")
}
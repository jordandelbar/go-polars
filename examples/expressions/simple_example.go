package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Load the iris dataset
	df, err := polars.ReadCSV("../../testdata/iris.csv")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== Simple Expression Operations Demo ===\n")

	// 1. Comparison Operations
	fmt.Println("1. Comparison Operations:")

	// Less than
	smallPetals := df.Filter(polars.Col("petal.length").Lt(2))
	fmt.Printf("   Flowers with petal length < 2: %d\n", smallPetals.Height())

	// Equal to
	mediumSepals := df.Filter(polars.Col("sepal.length").Eq(5))
	fmt.Printf("   Flowers with sepal length == 5: %d\n", mediumSepals.Height())

	// Greater than or equal to
	largePetals := df.Filter(polars.Col("petal.length").Ge(5))
	fmt.Printf("   Flowers with petal length >= 5: %d\n", largePetals.Height())

	// 2. Mathematical Operations
	fmt.Println("\n2. Mathematical Operations:")

	// Add constant to column
	df2 := df.WithColumns(
		polars.Col("petal.length").AddValue(10.0).Alias("petal_length_mm"),
	)
	fmt.Println("   Converting petal length to mm (add 10):")
	fmt.Println(df2.Select(polars.Col("petal.length"), polars.Col("petal_length_mm")).Head(3))

	// Multiply columns together
	df3 := df.WithColumns(
		polars.Col("petal.length").Mul(polars.Col("petal.width")).Alias("petal_area"),
	)
	fmt.Println("   Calculate petal area (length * width):")
	fmt.Println(df3.Select(
		polars.Col("petal.length"),
		polars.Col("petal.width"),
		polars.Col("petal_area"),
	).Head(3))

	// 3. Logical Operations
	fmt.Println("\n3. Logical Operations:")

	// AND operation
	bigFlowers := df.Filter(
		polars.Col("petal.length").Gt(4).And(polars.Col("sepal.length").Gt(6)),
	)
	fmt.Printf("   Big flowers (petal > 4 AND sepal > 6): %d\n", bigFlowers.Height())

	// OR operation
	extremes := df.Filter(
		polars.Col("petal.length").Lt(2).Or(polars.Col("petal.length").Gt(5)),
	)
	fmt.Printf("   Extreme petals (< 2 OR > 5): %d\n", extremes.Height())

	// NOT operation
	notSmall := df.Filter(polars.Col("petal.length").Lt(2).Not())
	fmt.Printf("   Not small petals (NOT < 2): %d\n", notSmall.Height())

	// 4. Chaining Operations
	fmt.Println("\n4. Chaining Multiple Operations:")

	result := df.
		Filter(polars.Col("petal.length").Gt(1)).                    // Filter: petal length > 1
		WithColumns(polars.Col("sepal.length").MulValue(2.0).Alias("doubled_sepal")). // Double sepal length
		Filter(polars.Col("doubled_sepal").Lt(12)).                  // Filter: doubled sepal < 12
		Select(polars.Col("variety"), polars.Col("sepal.length"), polars.Col("doubled_sepal"))

	fmt.Printf("   After chaining operations: %d rows\n", result.Height())
	fmt.Println("   Sample result:")
	fmt.Println(result.Head(3))

	fmt.Println("\n=== Demo Complete! ===")
}

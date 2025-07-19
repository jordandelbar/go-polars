package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	fmt.Println("=== go-polars Auto-Download Example ===")
	fmt.Println()

	// Check if the library initialized successfully
	if !polars.IsInitialized() {
		log.Fatalf("Failed to initialize go-polars: %v", polars.GetInitError())
	}

	fmt.Println("✅ go-polars initialized successfully!")
	fmt.Println("📦 Binary was automatically downloaded if needed")
	fmt.Println()

	// Load some sample data from CSV
	fmt.Println("📊 Loading sample data...")
	df, err := polars.ReadCSV("../data/example.csv")
	if err != nil {
		// If sample CSV doesn't exist, create a simple DataFrame programmatically
		fmt.Println("Sample CSV not found, creating data programmatically...")

		// Note: This is a simplified example. In a real implementation,
		// you'd need functions to create DataFrames from Go data structures
		fmt.Println("Creating sample data would require additional DataFrame creation functions")
		fmt.Println("For now, this example shows the auto-download functionality")
		return
	}

	// Display basic information
	fmt.Printf("DataFrame dimensions: %d rows × %d columns\n", df.Height(), df.Width())
	fmt.Printf("Columns: %v\n", df.Columns())
	fmt.Println()

	// Show first few rows
	fmt.Println("First 5 rows:")
	head := df.Head(5)
	fmt.Println(head.String())
	head.Free()

	// Example of column operations
	fmt.Println("\n🔍 Example operations:")

	// Select specific columns
	selected := df.Select(
		polars.Col("column1"),
		polars.Col("column2").Alias("renamed_col"),
	)
	fmt.Println("Selected columns:")
	fmt.Println(selected.String())
	selected.Free()

	// Filter data
	filtered := df.Filter(polars.Col("age").Gt(25))
	fmt.Printf("Filtered data (age > 25): %d rows\n", filtered.Height())
	filtered.Free()

	// Group by and aggregate
	groupby := df.GroupBy("category")
	aggregated := groupby.Agg(
		polars.Col("value").Sum().Alias("total_value"),
		polars.Count().Alias("count"),
	)
	fmt.Println("Grouped data:")
	fmt.Println(aggregated.String())
	aggregated.Free()
	groupby.Free()

	// Clean up
	df.Free()

	fmt.Println("\n🎉 Example completed successfully!")
	fmt.Println("💡 The binary was automatically downloaded and cached for future use")
}

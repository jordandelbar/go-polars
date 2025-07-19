package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	fmt.Println("🔄 Go-Polars Sorting Examples")
	fmt.Println("========================================")

	// Load sample data
	df, err := polars.ReadCSV("../data/iris.csv")
	if err != nil {
		log.Fatalf("Failed to read CSV: %v", err)
	}

	fmt.Printf("\n📊 Original DataFrame (%d rows, %d cols):\n", df.Height(), df.Width())
	fmt.Println(df.Head(10).String())

	// Example 1: Simple ascending sort by single column
	fmt.Println("\n🔸 Example 1: Sort by 'petal.length' (ascending)")
	sortedAsc := df.Sort("petal.length")
	fmt.Println(sortedAsc.Head(10).String())

	// Example 2: Single column descending sort
	fmt.Println("\n🔸 Example 2: Sort by 'petal.length' (descending)")
	sortedDesc := df.SortBy([]string{"petal.length"}, []bool{true})
	fmt.Println(sortedDesc.Head(10).String())

	// Example 3: Multi-column sort (all ascending)
	fmt.Println("\n🔸 Example 3: Sort by multiple columns (all ascending)")
	multiSort := df.Sort("variety", "petal.length", "sepal.width")
	fmt.Println(multiSort.Head(15).String())

	// Example 4: Multi-column sort with mixed order
	fmt.Println("\n🔸 Example 4: Sort by 'variety' (asc) and 'petal.length' (desc)")
	mixedSort := df.SortBy(
		[]string{"variety", "petal.length"},
		[]bool{false, true}, // variety ascending, petal.length descending
	)
	fmt.Println(mixedSort.Head(15).String())

	// Example 5: Sort by expressions
	fmt.Println("\n🔸 Example 5: Sort using expressions")
	exprSort := df.SortByExprs(
		[]polars.Expr{
			polars.Col("sepal.length"),
			polars.Col("petal.width"),
		},
		[]bool{false, true}, // sepal.length ascending, petal.width descending
	)
	fmt.Println(exprSort.Head(10).String())

	// Example 6: Complex chaining with sorting
	fmt.Println("\n🔸 Example 6: Complex operations with sorting")
	complex := df.
		Filter(polars.Col("petal.length").Gt(1)).
		WithColumns(
			polars.Col("petal.length").MulValue(2.0).Alias("doubled_petal"),
			polars.Col("sepal.length").AddValue(1.0).Alias("adjusted_sepal"),
		).
		SortBy(
			[]string{"variety", "doubled_petal"},
			[]bool{false, true}, // variety asc, doubled_petal desc
		).
		Select(
			polars.Col("variety"),
			polars.Col("petal.length"),
			polars.Col("doubled_petal"),
			polars.Col("adjusted_sepal"),
		)

	fmt.Printf("Filtered and sorted result (%d rows):\n", complex.Height())
	fmt.Println(complex.Head(20).String())

	// Example 7: Sort then aggregate
	fmt.Println("\n🔸 Example 7: Sort then group and aggregate")
	sortThenGroup := df.
		Sort("petal.length").
		GroupBy("variety").
		Agg(
			polars.Col("petal.length").Mean().Alias("avg_petal_length"),
			polars.Col("sepal.length").Max().Alias("max_sepal_length"),
			polars.Count().Alias("count"),
		)

	fmt.Println("Grouped results after sorting:")
	fmt.Println(sortThenGroup.String())

	// Example 8: Performance comparison - showing sorted vs unsorted data
	fmt.Println("\n🔸 Example 8: Performance insights")

	// First few rows of original data
	fmt.Println("Original data (first 5 rows):")
	original := df.Select(polars.Col("variety"), polars.Col("petal.length")).Head(5)
	fmt.Println(original.String())

	// Same selection but sorted
	fmt.Println("Sorted data (first 5 rows):")
	sorted := df.
		Sort("petal.length").
		Select(polars.Col("variety"), polars.Col("petal.length")).
		Head(5)
	fmt.Println(sorted.String())

	// Example 9: Sort with mathematical expressions
	fmt.Println("\n🔸 Example 9: Sort by computed values")
	computedSort := df.
		WithColumns(
			polars.Col("sepal.length").Add(polars.Col("petal.length")).Alias("total_length"),
		).
		Sort("total_length").
		Select(
			polars.Col("variety"),
			polars.Col("sepal.length"),
			polars.Col("petal.length"),
			polars.Col("total_length"),
		)

	fmt.Printf("Sorted by computed total_length (%d rows):\n", computedSort.Height())
	fmt.Println(computedSort.Head(10).String())

	// Example 10: Demonstrate sorting stability
	fmt.Println("\n🔸 Example 10: Multi-level sorting for data organization")
	organized := df.
		SortBy(
			[]string{"variety", "sepal.length", "petal.length"},
			[]bool{false, false, false}, // All ascending
		).
		Head(30)

	fmt.Println("Data organized by variety, then sepal length, then petal length:")
	fmt.Println(organized.String())

	fmt.Println("\n✅ Sorting examples completed!")
	fmt.Println("\nKey takeaways:")
	fmt.Println("- Use Sort() for simple ascending sorts")
	fmt.Println("- Use SortBy() for custom ascending/descending per column")
	fmt.Println("- Use SortByExprs() for expression-based sorting")
	fmt.Println("- Sorting integrates seamlessly with filtering, grouping, and selection")
	fmt.Println("- Multi-column sorting allows for sophisticated data organization")
}

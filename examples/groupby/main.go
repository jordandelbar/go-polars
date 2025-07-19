package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Load the iris dataset
	irisDf, err := polars.ReadCSV("../data/iris.csv")
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Original Iris DataFrame ===")
	fmt.Println(irisDf.Head(5))

	fmt.Println("\n=== GroupBy Operations ===")

	// Basic GroupBy Count
	fmt.Println("\n1. Count by variety:")
	gb1 := irisDf.GroupBy("variety")
	countResult := gb1.Count()
	fmt.Println(countResult)
	gb1.Free()

	// GroupBy Sum
	fmt.Println("\n2. Sum of petal.length by variety:")
	gb2 := irisDf.GroupBy("variety")
	sumResult := gb2.Sum("petal.length")
	fmt.Println(sumResult)
	gb2.Free()

	// GroupBy Mean
	fmt.Println("\n3. Mean of sepal.length by variety:")
	gb3 := irisDf.GroupBy("variety")
	meanResult := gb3.Mean("sepal.length")
	fmt.Println(meanResult)
	gb3.Free()

	// GroupBy Min
	fmt.Println("\n4. Minimum petal.width by variety:")
	gb4 := irisDf.GroupBy("variety")
	minResult := gb4.Min("petal.width")
	fmt.Println(minResult)
	gb4.Free()

	// GroupBy Max
	fmt.Println("\n5. Maximum petal.width by variety:")
	gb5 := irisDf.GroupBy("variety")
	maxResult := gb5.Max("petal.width")
	fmt.Println(maxResult)
	gb5.Free()

	// GroupBy Standard Deviation
	fmt.Println("\n6. Standard deviation of sepal.width by variety:")
	gb6 := irisDf.GroupBy("variety")
	stdResult := gb6.Std("sepal.width")
	fmt.Println(stdResult)
	gb6.Free()

	// Complex Aggregation with multiple operations
	fmt.Println("\n7. Multiple aggregations using Agg():")
	gb7 := irisDf.GroupBy("variety")
	aggResult := gb7.Agg(
		polars.Col("petal.length").Sum().Alias("petal_length_sum"),
		polars.Col("petal.width").Mean().Alias("petal_width_mean"),
		polars.Col("sepal.length").Max().Alias("sepal_length_max"),
		polars.Col("sepal.width").Min().Alias("sepal_width_min"),
		polars.Count().Alias("row_count"),
	)
	fmt.Println(aggResult)
	gb7.Free()

	// Demonstrating chained operations
	fmt.Println("\n8. Chained operations - Filter then GroupBy:")
	chainedResult := irisDf.
		Filter(polars.Col("petal.length").Gt(4)).
		GroupBy("variety").
		Agg(
			polars.Col("petal.length").Mean().Alias("avg_petal_length"),
			polars.Count().Alias("count_filtered"),
		)
	fmt.Println(chainedResult)

	// Statistics summary by group
	fmt.Println("\n9. Complete statistics summary by variety:")
	gb8 := irisDf.GroupBy("variety")
	statsResult := gb8.Agg(
		polars.Col("petal.length").Min().Alias("petal_length_min"),
		polars.Col("petal.length").Max().Alias("petal_length_max"),
		polars.Col("petal.length").Mean().Alias("petal_length_mean"),
		polars.Col("petal.length").Std().Alias("petal_length_std"),
		polars.Col("sepal.length").Min().Alias("sepal_length_min"),
		polars.Col("sepal.length").Max().Alias("sepal_length_max"),
		polars.Col("sepal.length").Mean().Alias("sepal_length_mean"),
		polars.Count().Alias("total_count"),
	)
	fmt.Println(statsResult)
	gb8.Free()

	// Save results to files
	fmt.Println("\n=== Saving Results ===")

	// Save the comprehensive stats
	err = statsResult.WriteCSV("iris_variety_stats.csv")
	if err != nil {
		log.Printf("Error saving stats CSV: %v", err)
	} else {
		fmt.Println("Comprehensive stats saved to 'iris_variety_stats.csv'")
	}

	// Save the aggregation result
	err = aggResult.WriteParquet("iris_variety_agg.parquet")
	if err != nil {
		log.Printf("Error saving aggregation Parquet: %v", err)
	} else {
		fmt.Println("Aggregation results saved to 'iris_variety_agg.parquet'")
	}

	fmt.Println("\n=== GroupBy Operations Demo Complete! ===")
	fmt.Printf("Original DataFrame: %d rows, %d columns\n", irisDf.Height(), irisDf.Width())
	fmt.Printf("Groups found: %d varieties\n", countResult.Height())
}

package main

import (
	"fmt"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Mock a small dataset instead of reading from file
	df, err := polars.NewDataFrame().
		AddStringColumn("variety", []string{"setosa", "setosa", "versicolor", "virginica", "virginica"}).
		AddFloatColumn("petal.length", []float64{1.4, 1.5, 4.7, 5.5, 5.9}).
		Build()
	if err != nil {
		panic(err)
	}

	gb := df.GroupBy("variety")

	aggs := gb.Agg(
		polars.Col("petal.length").Quantile(0.9).Alias("petal_length_q90"),
		polars.Col("petal.length").Var().Alias("petal_length_var"),
		polars.Col("petal.length").Median().Alias("petal_length_median"),
		polars.Col("petal.length").Product().Alias("petal_length_product"),
		// polars.Col("petal.length").First().Alias("petal_length_first"),
		// polars.Col("petal.length").Last().Alias("petal_length_last"),
		// polars.Col("variety").NUnique().Alias("variety_n_unique"),
		// polars.Col("variety").ApproxNUnique().Alias("variety_approx_n_unique"),
	)

	// Example: vertical concat (df stacked with itself)
	stacked := polars.ConcatRows(true, df, df)
	fmt.Println("Concatenated rows (double size):\n", stacked)

	// Example: lazy + streaming collect
	lf := df.Lazy()
	defer lf.Free()
	streamed := lf.Collect(true)
	fmt.Println("Streaming collect:\n", streamed)

	fmt.Println("Advanced aggregations by variety:\n", aggs)
}

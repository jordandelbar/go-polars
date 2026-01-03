package main

import (
	"fmt"

	"github.com/jordandelbar/go-polars/polars"
)

func preview(label string, df *polars.DataFrame) {
	fmt.Printf("\n%s (%d rows x %d cols)\n", label, df.Height(), df.Width())
	fmt.Println(df.Head(5))
}

func main() {
	irisDf, err := polars.ReadCSV("../data/iris.csv")
	if err != nil {
		panic(err)
	}

	preview("Original iris sample", irisDf)

	petalLengthBucket := polars.When(polars.Col("petal.length").Gt(4.5)).
		ThenValue(int64(2)).
		When(polars.Col("petal.length").Gt(3.0)).
		ThenValue(int64(1)).
		OtherwiseValue(int64(0))

	petalLengthLabel := polars.When(polars.Col("petal.length").Gt(4.5)).
		ThenValue("long").
		When(polars.Col("petal.length").Gt(3.0)).
		ThenValue("medium").
		OtherwiseValue("short")

	petalWidthScore := polars.When(polars.Col("petal.width").Gt(1.8)).
		Then(polars.Col("petal.width").MulValue(100)).
		Otherwise(polars.Col("petal.width").MulValue(10))

	enriched := irisDf.WithColumns(
		petalLengthBucket.Alias("petal_length_bucket"),
		petalLengthLabel.Alias("petal_length_label"),
		petalWidthScore.Alias("petal_width_alert_score"),
	)

	preview("Enriched with When/Then/Otherwise", enriched)

	mediumOrLong := enriched.Filter(
		polars.Col("petal_length_bucket").Ge(1),
	)

	preview("Medium or long petals", mediumOrLong)
}

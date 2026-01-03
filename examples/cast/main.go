package main

import (
	"fmt"

	pl "github.com/jordandelbar/go-polars/polars"
)

func printSchema(df *pl.DataFrame, label string) {
	fmt.Printf("%s schema:\n", label)
	for _, col := range df.Schema() {
		fmt.Printf("  %-15s %s\n", col.Name, col.Type)
	}
	fmt.Println()
}

func main() {
	irisDf, err := pl.ReadCSV("../data/iris.csv")
	if err != nil {
		panic(err)
	}

	printSchema(irisDf, "original")

	casted := irisDf.WithColumns(
		pl.Col("petal.length").Cast(pl.DataTypeUTF8).Alias("petal_length_utf8"),
	)

	printSchema(casted, "after cast")

	fmt.Println(casted.Head(5))
}

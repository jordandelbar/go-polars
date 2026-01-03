package main

import (
	"fmt"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Case 1: nil DataFrame pointer
	var nilDF *pl.DataFrame
	fmt.Println("nil DataFrame is nil?", nilDF == nil)
	fmt.Println("nil DataFrame IsEmpty?", nilDF == nil || nilDF.IsEmpty())

	// Case 2: empty DataFrame (0 rows) built from empty slices
	df, err := pl.NewDataFrame().
		AddStringColumn("col", []string{}).
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Println("empty DataFrame IsEmpty?", df.IsEmpty())
}

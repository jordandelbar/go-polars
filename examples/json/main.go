package main

import (
	"fmt"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Build a small DataFrame in-memory
	df, err := pl.NewDataFrame().
		AddStringColumn("name", []string{"Alice", "Bob"}).
		AddIntColumn("age", []int64{30, 45}).
		AddBoolColumn("active", []bool{true, false}).
		Build()
	if err != nil {
		panic(err)
	}

	outPath := "./people.json"
	if err := df.WriteJSON(outPath); err != nil {
		panic(err)
	}

	// Read it back from disk
	readBack, err := pl.ReadJSON(outPath)
	if err != nil {
		panic(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)

	fmt.Println("\nRead back from JSON:")
	fmt.Println(readBack)

	// Clean up the temp file so the example is repeatable
	// _ = os.Remove(outPath)
}

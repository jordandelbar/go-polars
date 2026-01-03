package main

import (
	"fmt"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	// hasHeader := true

	lf := pl.ScanCSVWithOptions("./test.csv", pl.ScanCSVOptions{
		Overrides: map[string]pl.DataType{
			"创建时间": pl.DataTypeDate,
		},
	})
	df := lf.Collect(true)
	fmt.Println(df.Head(100))
}

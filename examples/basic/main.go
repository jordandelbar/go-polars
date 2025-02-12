package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	df, err := polars.ReadCSV("../../testdata/iris.csv")
	if err != nil {
		panic(err)
	}

	filteredDf := df.Filter(polars.Col("petal.length").Gt(1))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("5 first rows of df", df.Head(5))
	fmt.Println("whole df", df)
	dfColumns := df.Columns()
	found := false
	for _, col := range dfColumns {
		if col == "variety" {
			found = true
			break
		}

	}
	if found {
		fmt.Println("Column `variety` is there")
	}

	firstRowsDfGt1 := df.Head(5).Filter(polars.Col("petal.length").Gt(1)).WithColumns(polars.Lit("hello").Alias("test"))
	fmt.Println(firstRowsDfGt1)

	err = filteredDf.WriteCSV("output.csv")
	if err != nil {
		fmt.Println("Problem when writing csv")
	}

	fmt.Println("5 first rows of filteredDf", filteredDf.Head(5))
	fmt.Println("Original df", df)
	fmt.Println("Original df columns", df.Columns())
}

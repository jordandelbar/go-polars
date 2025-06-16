package main

import (
	"fmt"
	"log"
	"slices"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	irisDf, err := polars.ReadCSV("../data/iris.csv")
	if err != nil {
		panic(err)
	}

	petalLengthGreaterThanOne := irisDf.Filter(polars.Col("petal.length").Gt(1))

	fmt.Println("whole iris dataframe", irisDf)
	fmt.Println("5 first rows of iris dataframe", irisDf.Head(5))
	irisDfColumns := irisDf.Columns()
	found := slices.Contains(irisDfColumns, "variety")

	if found {
		fmt.Println("Column `variety` is there")
	}

	petalLengthGreaterThanOneFirstRows := irisDf.Head(5).Filter(polars.Col("petal.length").Gt(1)).WithColumns(polars.Lit("hello").Alias("test"))
	fmt.Println(petalLengthGreaterThanOneFirstRows)

	err = petalLengthGreaterThanOne.WriteCSV("output.csv")
	if err != nil {
		fmt.Println("Problem when writing csv")
	}

	selectDf := irisDf.Select(polars.Col("petal.length"), polars.Col("sepal.width"))
	fmt.Println("Petal length and sepal width\n", selectDf)

	fmt.Println("5 first rows of petalLengthGreaterThanOne\n", petalLengthGreaterThanOne.Head(5))
	fmt.Println("Original irisDf\n", irisDf)
	fmt.Println("Original irisDf columns\n", irisDf.Columns())

	err = petalLengthGreaterThanOne.WriteParquet("output.parquet")
	if err != nil {
		log.Fatal(err)
	}

	readBackDf, err := polars.ReadParquet("output.parquet")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Parquet file: \n", readBackDf)
}

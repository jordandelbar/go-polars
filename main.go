package main

import (
	"fmt"
	"log"
	"polars_go/polars"
)

func main() {
	df, err := polars.ReadCSV("data.csv")
	if err != nil {
		panic(err)
	}

	filteredDf, err := df.Filter(polars.Col("blub").Gt(1))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("5 first rows of df", df.Head(5))
	fmt.Println("whole df", df)
	dfColumns := df.Columns()
	found := false
	for _, col := range dfColumns {
		if col == "blub" {
			found = true
			break
		}

	}

	if found {
		fmt.Println("Blub is there")
	}

	err = filteredDf.WriteCSV("output.csv")
	if err != nil {
		fmt.Println("Problem when writing csv")
	}

	fmt.Println("5 first rows of filteredDf", filteredDf.Head(5))
}

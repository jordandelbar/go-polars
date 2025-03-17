package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

type FlightRecord struct {
	FLDate    int16     `json:"fl_date"`
	DepDelay  int16     `json:"dep_delay"`
	ArrDelay  int16     `json:"arr_delay"`
	AirTime   int16     `json:"air_time"`
	Distance  int16     `json:"distance"`
	DepTime   float32   `json:"dep_time"`
	ArrTime   float32   `json:"arr_time"`
}

func main() {
	irisDf, err := polars.ReadCSV("../../testdata/iris.csv")
	if err != nil {
		panic(err)
	}

	petalLengthGreaterThanOne := irisDf.Filter(polars.Col("petal.length").Gt(1))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("whole iris dataframe", irisDf)
	fmt.Println("5 first rows of iris dataframe", irisDf.Head(5))
	irisDfColumns := irisDf.Columns()
	found := false
	for _, col := range irisDfColumns {
		if col == "variety" {
			found = true
			break
		}

	}
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

	df, err := polars.ReadParquet("../../testdata/flights-1m.parquet")
	if err != nil {
		log.Fatal(err)
	}

	iter := df.IterRows()
	defer iter.Free()
	data, _ := iter.NextJson()
	
	var flightRecord FlightRecord

	if err := json.Unmarshal([]byte(data), &flightRecord); err != nil{
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", flightRecord)

}

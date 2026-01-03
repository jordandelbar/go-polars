package main

import (
	"fmt"
	"log"
	"time"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	df, err := pl.DataFrameFromMap(map[string]interface{}{
		"name": []string{"Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"},
		"birthdate": []time.Time{
			time.Date(1997, 1, 10, 0, 0, 0, 0, time.UTC),
			time.Date(1985, 2, 15, 0, 0, 0, 0, time.UTC),
			time.Date(1983, 3, 22, 0, 0, 0, 0, time.UTC),
			time.Date(1981, 4, 30, 0, 0, 0, 0, time.UTC),
		},
		"weight": []float64{57.9, 72.5, 53.6, 83.1},
		"height": []float64{1.56, 1.77, 1.65, 1.75},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(df)
	df = df.WithColumns(pl.Col("weight").Cast(pl.DataTypeUTF8).Alias("new"))
	fmt.Println(df)
}

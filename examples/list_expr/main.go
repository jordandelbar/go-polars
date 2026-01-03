package main

import (
	"fmt"
	"time"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	df, err := pl.NewDataFrame().
		AddListStringColumn("names", [][]string{
			{"Anne", "Averill", "Adams"},
			{"Brandon", "Brooke", "Borden", "Branson"},
			{"Camila", "Campbell"},
			{"Dennis", "Doyle"},
		}).
		AddListIntColumn("children_ages", [][]int64{
			{5, 7},
			{},
			{},
			{8, 11, 18},
		}).
		AddListDatetimeMsColumn("medical_appointments", [][]time.Time{
			{},
			{},
			{},
			{time.Date(2022, 5, 22, 16, 30, 0, 0, time.UTC)},
		}).
		Build()
	if err != nil {
		panic(err)
	}

	// Demonstrate list helpers on the names column
	namesList := pl.Col("names")

	listOps := df.WithColumns(
		namesList.Alias("names_list"),
		namesList.List().Len().Alias("names_len"),
		// namesList.List().First().Alias("first_name"),
		// namesList.List().Last().Alias("last_name"),
		// namesList.List().Get(1).Alias("second_name"),
		namesList.List().Join("|").Alias("names_joined"),
		// namesList.List().Sort(false, false).Alias("names_sorted"),
		// namesList.List().Reverse().Alias("names_reversed"),
		// namesList.List().Unique(true).Alias("names_unique"),
		// namesList.List().Head(2).Alias("names_head2"),
		// namesList.List().Tail(2).Alias("names_tail2"),
		namesList.List().Slice(1, 2).Alias("names_slice_1_2"),
	)

	fmt.Println("List helpers demo (all rows):")
	fmt.Println(listOps)
}

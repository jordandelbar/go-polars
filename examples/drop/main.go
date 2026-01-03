package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	// Build a small mock employee dataset in memory
	employees, err := polars.NewDataFrame().
		AddStringColumn("name", []string{"Alice", "Bob", "Carol", "Dylan"}).
		AddStringColumn("department", []string{"Engineering", "Sales", "Marketing", "Engineering"}).
		AddStringColumn("city", []string{"NYC", "LA", "Chicago", "NYC"}).
		AddBoolColumn("remote", []bool{true, false, true, false}).
		AddFloatColumn("salary", []float64{120000, 95000, 105000, 130000}).
		Build()
	if err != nil {
		log.Fatalf("failed to build mock dataframe: %v", err)
	}
	defer employees.Free()

	fmt.Println("=== Original DataFrame ===")
	fmt.Printf("Columns: %v\n", employees.Columns())
	fmt.Println(employees.String())

	// Drop a single column
	noCity := employees.Drop("city")
	defer noCity.Free()
	fmt.Println("\n=== After dropping `city` ===")
	fmt.Printf("Columns: %v\n", noCity.Columns())
	fmt.Println(noCity.String())

	// Drop multiple columns at once
	compact := employees.Drop("city", "remote")
	defer compact.Free()
	fmt.Println("\n=== After dropping `city` and `remote` ===")
	fmt.Printf("Columns: %v\n", compact.Columns())
	fmt.Println(compact.String())
}

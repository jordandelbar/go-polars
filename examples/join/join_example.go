package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	fmt.Println("=== Go-Polars Join Operations Example ===")

	// Create employee DataFrame
	fmt.Println("Creating employee DataFrame...")
	employeeBuilder := polars.NewDataFrame()
	employees, err := employeeBuilder.
		AddIntColumn("id", []int64{1, 2, 3, 4, 5}).
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}).
		AddStringColumn("role", []string{"Engineer", "Manager", "Engineer", "Designer", "Engineer"}).
		AddIntColumn("salary", []int64{75000, 85000, 70000, 65000, 72000}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create employee DataFrame: %v", err)
	}
	defer employees.Free()

	fmt.Printf("Employees:\n%s\n", employees.String())

	// Create department DataFrame
	fmt.Println("Creating department DataFrame...")
	deptBuilder := polars.NewDataFrame()
	departments, err := deptBuilder.
		AddIntColumn("emp_id", []int64{1, 2, 3, 6}).
		AddStringColumn("department", []string{"Engineering", "Sales", "Engineering", "Marketing"}).
		AddStringColumn("location", []string{"San Francisco", "New York", "San Francisco", "Boston"}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create department DataFrame: %v", err)
	}
	defer departments.Free()

	fmt.Printf("Departments:\n%s\n", departments.String())

	// Example 1: Inner Join
	fmt.Println("=== Example 1: Inner Join ===")
	fmt.Println("Only employees who have department assignments")
	innerJoin := employees.JoinOn(departments, "id", "emp_id", polars.JoinInner)
	defer innerJoin.Free()
	fmt.Printf("Inner Join Result:\n%s\n", innerJoin.String())

	// Example 2: Left Join
	fmt.Println("=== Example 2: Left Join ===")
	fmt.Println("All employees, with department info where available")
	leftJoin := employees.JoinOn(departments, "id", "emp_id", polars.JoinLeft)
	defer leftJoin.Free()
	fmt.Printf("Left Join Result:\n%s\n", leftJoin.String())

	// Example 3: Right Join
	fmt.Println("=== Example 3: Right Join ===")
	fmt.Println("All department assignments, with employee info where available")
	rightJoin := employees.JoinOn(departments, "id", "emp_id", polars.JoinRight)
	defer rightJoin.Free()
	fmt.Printf("Right Join Result:\n%s\n", rightJoin.String())

	// Example 4: Outer Join
	fmt.Println("=== Example 4: Outer Join ===")
	fmt.Println("All employees and all department assignments")
	outerJoin := employees.JoinOn(departments, "id", "emp_id", polars.JoinOuter)
	defer outerJoin.Free()
	fmt.Printf("Outer Join Result:\n%s\n", outerJoin.String())

	// Example 5: Multiple Key Join
	fmt.Println("=== Example 5: Multiple Key Join ===")
	fmt.Println("Creating DataFrames for multiple key join example...")

	// Create sales data
	salesBuilder := polars.NewDataFrame()
	sales, err := salesBuilder.
		AddIntColumn("year", []int64{2023, 2023, 2024, 2024}).
		AddStringColumn("quarter", []string{"Q1", "Q2", "Q1", "Q2"}).
		AddIntColumn("revenue", []int64{100000, 120000, 110000, 130000}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create sales DataFrame: %v", err)
	}
	defer sales.Free()

	// Create costs data
	costsBuilder := polars.NewDataFrame()
	costs, err := costsBuilder.
		AddIntColumn("year", []int64{2023, 2023, 2024}).
		AddStringColumn("quarter", []string{"Q1", "Q2", "Q1"}).
		AddIntColumn("costs", []int64{80000, 90000, 85000}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create costs DataFrame: %v", err)
	}
	defer costs.Free()

	fmt.Printf("Sales:\n%s\n", sales.String())
	fmt.Printf("Costs:\n%s\n", costs.String())

	// Join on multiple columns (year and quarter)
	multiKeyJoin := sales.JoinMultiple(costs, "year,quarter", "year,quarter", polars.JoinInner)
	defer multiKeyJoin.Free()
	fmt.Printf("Multiple Key Join Result:\n%s\n", multiKeyJoin.String())

	// Example 6: Join with Operations
	fmt.Println("=== Example 6: Join with Additional Operations ===")
	fmt.Println("Join and calculate profit margin")

	profitAnalysis := sales.
		JoinMultiple(costs, "year,quarter", "year,quarter", polars.JoinInner).
		WithColumns(
			polars.Col("revenue").Sub(polars.Col("costs")).Alias("profit"),
		).
		WithColumns(
			polars.Col("profit").Div(polars.Col("revenue")).MulValue(100.0).Alias("profit_margin_pct"),
		).
		Sort("year", "quarter")

	defer profitAnalysis.Free()
	fmt.Printf("Profit Analysis:\n%s\n", profitAnalysis.String())

	fmt.Println("=== Join Examples Complete ===")
}

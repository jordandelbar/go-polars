package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	fmt.Println("=== DataFrame Creation Examples ===")

	// Example 1: Create a mixed-type DataFrame (like Python Polars)
	fmt.Println("\n🔸 Example 1: Mixed-type DataFrame")

	df1, err := polars.NewDataFrameBuilder().
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie", "Diana"}).
		AddIntColumn("age", []int64{25, 30, 35, 28}).
		AddFloatColumn("salary", []float64{50000.5, 60000.75, 70000.25, 55000.0}).
		AddBoolColumn("is_active", []bool{true, false, true, true}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Printf("Created DataFrame with %d rows and %d columns\n", df1.Height(), df1.Width())
	fmt.Printf("Columns: %v\n", df1.Columns())
	fmt.Println(df1.String())

	// Example 2: Employee data with operations
	fmt.Println("\n🔸 Example 2: Employee data with filtering and calculations")

	employees, err := polars.NewDataFrameBuilder().
		AddStringColumn("department", []string{"Engineering", "Sales", "Marketing", "Engineering", "Sales"}).
		AddStringColumn("employee", []string{"John", "Jane", "Bob", "Alice", "Charlie"}).
		AddIntColumn("years_experience", []int64{5, 3, 7, 2, 4}).
		AddFloatColumn("base_salary", []float64{80000, 55000, 65000, 70000, 60000}).
		AddBoolColumn("remote_work", []bool{true, false, true, true, false}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create employees DataFrame: %v", err)
	}

	fmt.Println("All employees:")
	fmt.Println(employees.String())

	// Filter for senior employees (5+ years experience)
	seniorEmployees := employees.Filter(polars.Col("years_experience").Ge(5))
	fmt.Println("\nSenior employees (5+ years experience):")
	fmt.Println(seniorEmployees.String())

	// Calculate annual salary including bonus (10% of base for remote workers)
	withBonus := employees.WithColumns(
		polars.Col("base_salary").
			Add(polars.Col("base_salary").MulValue(0.1)).
			Alias("total_compensation"),
	)
	fmt.Println("\nWith bonus calculations:")
	fmt.Println(withBonus.String())

	// Example 3: Product inventory
	fmt.Println("\n🔸 Example 3: Product inventory management")

	inventory, err := polars.NewDataFrameBuilder().
		AddStringColumn("product_id", []string{"LAPTOP001", "MOUSE002", "KEYBOARD003", "MONITOR004"}).
		AddStringColumn("category", []string{"Electronics", "Accessories", "Accessories", "Electronics"}).
		AddIntColumn("quantity", []int64{50, 200, 75, 30}).
		AddFloatColumn("unit_price", []float64{999.99, 29.99, 79.99, 299.99}).
		AddBoolColumn("in_stock", []bool{true, true, true, false}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create inventory DataFrame: %v", err)
	}

	// Calculate total value of inventory
	inventoryValue := inventory.WithColumns(
		polars.Col("quantity").Mul(polars.Col("unit_price")).Alias("total_value"),
	)

	fmt.Println("Product inventory with total values:")
	fmt.Println(inventoryValue.String())

	// Filter for high-value items (>$10,000 total value)
	highValueItems := inventoryValue.Filter(polars.Col("total_value").Gt(10000.0))
	fmt.Println("\nHigh-value inventory items:")
	fmt.Println(highValueItems.String())

	// Example 4: Time series data
	fmt.Println("\n🔸 Example 4: Sales performance data")

	sales, err := polars.NewDataFrameBuilder().
		AddStringColumn("month", []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun"}).
		AddIntColumn("units_sold", []int64{120, 135, 98, 167, 189, 203}).
		AddFloatColumn("revenue", []float64{12000.50, 13500.75, 9800.25, 16700.00, 18900.50, 20300.75}).
		AddFloatColumn("profit_margin", []float64{0.15, 0.18, 0.12, 0.20, 0.22, 0.25}).
		AddBoolColumn("target_met", []bool{false, true, false, true, true, true}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create sales DataFrame: %v", err)
	}

	// Calculate profit and growth metrics
	salesAnalysis := sales.WithColumns(
		polars.Col("revenue").Mul(polars.Col("profit_margin")).Alias("profit"),
		polars.Col("revenue").Div(polars.Col("units_sold")).Alias("avg_price_per_unit"),
	)

	fmt.Println("Sales analysis with calculated metrics:")
	fmt.Println(salesAnalysis.String())

	// Find months where targets were met
	targetMet := salesAnalysis.Filter(polars.Col("target_met").Eq(true))
	fmt.Println("\nMonths where sales targets were met:")
	fmt.Println(targetMet.String())

	// Example 5: Complex chaining operations
	fmt.Println("\n🔸 Example 5: Complex data processing pipeline")

	// Start with raw data
	rawData, err := polars.NewDataFrameBuilder().
		AddStringColumn("customer_type", []string{"Premium", "Standard", "Premium", "Basic", "Standard", "Premium"}).
		AddIntColumn("transaction_count", []int64{45, 12, 38, 5, 18, 52}).
		AddFloatColumn("total_spent", []float64{4500.00, 800.00, 3200.00, 150.00, 1200.00, 5100.00}).
		AddBoolColumn("is_member", []bool{true, false, true, false, false, true}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create raw data DataFrame: %v", err)
	}

	// Complex processing pipeline
	processedData := rawData.
		// Add calculated columns
		WithColumns(
			polars.Col("total_spent").Div(polars.Col("transaction_count")).Alias("avg_transaction"),
			polars.Col("total_spent").MulValue(0.05).Alias("loyalty_points"),
		).
		// Filter for valuable customers
		Filter(polars.Col("avg_transaction").Gt(50.0)).
		// Select relevant columns
		Select(
			polars.Col("customer_type"),
			polars.Col("avg_transaction"),
			polars.Col("loyalty_points"),
			polars.Col("is_member"),
		).
		// Sort by average transaction value
		SortBy([]string{"avg_transaction"}, []bool{true}) // descending

	fmt.Println("Processed customer data (high-value customers):")
	fmt.Println(processedData.String())

	fmt.Println("\n✅ DataFrame creation examples completed!")

	// Clean up
	df1.Free()
	employees.Free()
	inventory.Free()
	sales.Free()
	rawData.Free()
}

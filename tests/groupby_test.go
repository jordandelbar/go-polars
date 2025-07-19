package tests

import (
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

func TestGroupByBasic(t *testing.T) {
	// Load test data
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Test GroupBy creation
	gb := df.GroupBy("variety")
	if gb == nil {
		t.Fatal("GroupBy should not be nil")
	}
	defer gb.Free()

	// Test Count aggregation
	countResult := gb.Count()
	if countResult == nil {
		t.Fatal("Count result should not be nil")
	}

	// Check that we have 3 groups (3 varieties in iris dataset)
	if countResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", countResult.Height())
	}

	// Check that we have the expected columns
	columns := countResult.Columns()
	expectedColumns := []string{"variety", "count"}
	if len(columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
	}

	for i, expected := range expectedColumns {
		if i >= len(columns) || columns[i] != expected {
			t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
		}
	}
}

func TestGroupBySum(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	gb := df.GroupBy("variety")
	defer gb.Free()

	// Test Sum aggregation
	sumResult := gb.Sum("petal.length")
	if sumResult == nil {
		t.Fatal("Sum result should not be nil")
	}

	// Should have 3 groups
	if sumResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", sumResult.Height())
	}

	// Should have variety and petal.length columns
	columns := sumResult.Columns()
	if len(columns) < 2 {
		t.Errorf("Expected at least 2 columns, got %d", len(columns))
	}
}

func TestGroupByMean(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	gb := df.GroupBy("variety")
	defer gb.Free()

	// Test Mean aggregation
	meanResult := gb.Mean("sepal.length")
	if meanResult == nil {
		t.Fatal("Mean result should not be nil")
	}

	// Should have 3 groups
	if meanResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", meanResult.Height())
	}

	columns := meanResult.Columns()
	if len(columns) < 2 {
		t.Errorf("Expected at least 2 columns, got %d", len(columns))
	}
}

func TestGroupByMinMax(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	gb := df.GroupBy("variety")
	defer gb.Free()

	// Test Min aggregation
	minResult := gb.Min("petal.width")
	if minResult == nil {
		t.Fatal("Min result should not be nil")
	}

	if minResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", minResult.Height())
	}

	// Test Max aggregation
	gb2 := df.GroupBy("variety")
	defer gb2.Free()

	maxResult := gb2.Max("petal.width")
	if maxResult == nil {
		t.Fatal("Max result should not be nil")
	}

	if maxResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", maxResult.Height())
	}
}

func TestGroupByStd(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	gb := df.GroupBy("variety")
	defer gb.Free()

	// Test Standard Deviation aggregation
	stdResult := gb.Std("sepal.width")
	if stdResult == nil {
		t.Fatal("Std result should not be nil")
	}

	if stdResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", stdResult.Height())
	}

	columns := stdResult.Columns()
	if len(columns) < 2 {
		t.Errorf("Expected at least 2 columns, got %d", len(columns))
	}
}

func TestGroupByMultipleColumns(t *testing.T) {
	// Create a simple test DataFrame first
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Test grouping by multiple columns (though iris only has one categorical column)
	// We'll group by variety which should still work
	gb := df.GroupBy("variety")
	defer gb.Free()

	countResult := gb.Count()
	if countResult == nil {
		t.Fatal("Count result should not be nil")
	}

	if countResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", countResult.Height())
	}
}

func TestGroupByAgg(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	gb := df.GroupBy("variety")
	defer gb.Free()

	// Test Agg with multiple expressions
	aggResult := gb.Agg(
		polars.Col("petal.length").Sum(),
		polars.Col("petal.width").Mean(),
		polars.Col("sepal.length").Max(),
		polars.Col("sepal.width").Min(),
	)

	if aggResult == nil {
		t.Fatal("Agg result should not be nil")
	}

	if aggResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", aggResult.Height())
	}

	// Should have variety column plus 4 aggregated columns
	columns := aggResult.Columns()
	if len(columns) < 5 {
		t.Errorf("Expected at least 5 columns, got %d", len(columns))
	}
}

func TestGroupByWithCount(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	gb := df.GroupBy("variety")
	defer gb.Free()

	// Test Agg with Count expression
	aggResult := gb.Agg(
		polars.Count(),
		polars.Col("petal.length").Mean(),
	)

	if aggResult == nil {
		t.Fatal("Agg result should not be nil")
	}

	if aggResult.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", aggResult.Height())
	}
}

func TestGroupByChaining(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Test chaining operations after GroupBy
	result := df.
		Filter(polars.Col("petal.length").Gt(1)).
		GroupBy("variety").
		Sum("petal.length")

	if result == nil {
		t.Fatal("Chained result should not be nil")
	}

	// Should still have groups, possibly fewer due to filtering
	if result.Height() == 0 {
		t.Error("Expected some groups after filtering and grouping")
	}
}

func TestGroupByErrorHandling(t *testing.T) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Test GroupBy with non-existent column
	gb := df.GroupBy("non_existent_column")
	defer gb.Free()

	// This should handle the error gracefully
	result := gb.Count()
	// The result might be nil or empty, but should not crash
	_ = result
}

func BenchmarkGroupByCount(b *testing.B) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		b.Fatalf("Failed to read CSV: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gb := df.GroupBy("variety")
		result := gb.Count()
		gb.Free()
		_ = result
	}
}

func BenchmarkGroupBySum(b *testing.B) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		b.Fatalf("Failed to read CSV: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gb := df.GroupBy("variety")
		result := gb.Sum("petal.length")
		gb.Free()
		_ = result
	}
}

func BenchmarkGroupByAgg(b *testing.B) {
	df, err := polars.ReadCSV("../examples/data/iris.csv")
	if err != nil {
		b.Fatalf("Failed to read CSV: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gb := df.GroupBy("variety")
		result := gb.Agg(
			polars.Col("petal.length").Sum(),
			polars.Col("petal.width").Mean(),
		)
		gb.Free()
		_ = result
	}
}

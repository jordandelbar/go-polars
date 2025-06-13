package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

func TestMain(m *testing.M) {
	// Setup: Build the library before running tests
	// This ensures the binary is available for testing
	code := m.Run()
	os.Exit(code)
}

func getTestDataPath() string {
	// Get the path to the test data file
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "testdata", "iris.csv")
}

func loadTestData(t *testing.T) *polars.DataFrame {
	df, err := polars.ReadCSV(getTestDataPath())
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}
	return df
}

// Test Comparison Operations
func TestComparisonOperations(t *testing.T) {
	df := loadTestData(t)
	originalHeight := df.Height()

	t.Run("GreaterThan", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Gt(5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length > 5")
		}
		if filtered.Height() >= originalHeight {
			t.Error("Filtered result should have fewer rows than original")
		}
	})

	t.Run("LessThan", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Lt(2))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length < 2")
		}
		if filtered.Height() >= originalHeight {
			t.Error("Filtered result should have fewer rows than original")
		}
	})

	t.Run("EqualTo", func(t *testing.T) {
		filtered := df.Filter(polars.Col("sepal.length").Eq(5))
		// We expect some rows, but not all
		if filtered.Height() >= originalHeight {
			t.Error("Filtered result should have fewer rows than original")
		}
	})

	t.Run("NotEqualTo", func(t *testing.T) {
		filtered := df.Filter(polars.Col("sepal.length").Ne(5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with sepal.length != 5")
		}
	})

	t.Run("GreaterThanOrEqual", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Ge(5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length >= 5")
		}
	})

	t.Run("LessThanOrEqual", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Le(2))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length <= 2")
		}
	})
}

// Test Mathematical Operations
func TestMathematicalOperations(t *testing.T) {
	df := loadTestData(t)

	t.Run("AddValue", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("petal.length").AddValue(1.0).Alias("petal_plus_one"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}

		columns := result.Columns()
		found := false
		for _, col := range columns {
			if col == "petal_plus_one" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find 'petal_plus_one' column")
		}
	})

	t.Run("SubValue", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("sepal.length").SubValue(0.5).Alias("sepal_minus_half"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}
	})

	t.Run("MulValue", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("petal.width").MulValue(2.0).Alias("petal_width_doubled"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}
	})

	t.Run("DivValue", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("sepal.width").DivValue(2.0).Alias("sepal_width_halved"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}
	})

	t.Run("AddColumns", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("petal.length").Add(polars.Col("petal.width")).Alias("petal_sum"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}

		columns := result.Columns()
		found := false
		for _, col := range columns {
			if col == "petal_sum" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find 'petal_sum' column")
		}
	})

	t.Run("SubColumns", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("sepal.length").Sub(polars.Col("sepal.width")).Alias("sepal_diff"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}
	})

	t.Run("MulColumns", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("petal.length").Mul(polars.Col("petal.width")).Alias("petal_area"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}
	})

	t.Run("DivColumns", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("sepal.length").Div(polars.Col("sepal.width")).Alias("sepal_ratio"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}
	})
}

// Test Logical Operations
func TestLogicalOperations(t *testing.T) {
	df := loadTestData(t)

	t.Run("And", func(t *testing.T) {
		// Test AND operation: petal.length > 4 AND petal.width > 1
		result := df.Filter(
			polars.Col("petal.length").Gt(4).And(polars.Col("petal.width").Gt(1)),
		)

		if result.Height() == 0 {
			t.Error("Expected some rows matching both conditions")
		}

		if result.Height() >= df.Height() {
			t.Error("AND result should have fewer rows than original")
		}
	})

	t.Run("Or", func(t *testing.T) {
		// Test OR operation: petal.length < 2 OR petal.width > 2
		result := df.Filter(
			polars.Col("petal.length").Lt(2).Or(polars.Col("petal.width").Gt(2)),
		)

		if result.Height() == 0 {
			t.Error("Expected some rows matching either condition")
		}
	})

	t.Run("Not", func(t *testing.T) {
		// Test NOT operation: NOT(petal.length > 4)
		result := df.Filter(
			polars.Col("petal.length").Gt(4).Not(),
		)

		if result.Height() == 0 {
			t.Error("Expected some rows where petal.length is NOT > 4")
		}

		if result.Height() >= df.Height() {
			t.Error("NOT result should have fewer rows than original")
		}
	})
}

// Test Complex Expression Combinations
func TestComplexExpressions(t *testing.T) {
	df := loadTestData(t)

	t.Run("ComplexFilter", func(t *testing.T) {
		// Complex filter: (petal.length > 3 AND petal.width > 1) OR (sepal.length < 5)
		result := df.Filter(
			polars.Col("petal.length").Gt(3).And(polars.Col("petal.width").Gt(1)).Or(
				polars.Col("sepal.length").Lt(5),
			),
		)

		if result.Height() == 0 {
			t.Error("Expected some rows matching complex condition")
		}
	})

	t.Run("ComplexMathematicalExpression", func(t *testing.T) {
		// Complex math: (petal.length * 2) + petal.width - 1
		result := df.WithColumns(
			polars.Col("petal.length").MulValue(2.0).Add(polars.Col("petal.width")).SubValue(1.0).Alias("complex_calc"),
		)

		if result.Height() != df.Height() {
			t.Error("Result should have same number of rows as original")
		}

		columns := result.Columns()
		found := false
		for _, col := range columns {
			if col == "complex_calc" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find 'complex_calc' column")
		}
	})

	t.Run("ChainedOperations", func(t *testing.T) {
		// Chain multiple operations
		result := df.
			Filter(polars.Col("petal.length").Gt(1)).
			WithColumns(polars.Col("petal.length").MulValue(10.0).Alias("petal_length_mm")).
			Filter(polars.Col("petal_length_mm").Le(50)).
			Select(polars.Col("variety"), polars.Col("petal.length"), polars.Col("petal_length_mm"))

		if result.Height() == 0 {
			t.Error("Expected some rows after chained operations")
		}

		if result.Width() != 3 {
			t.Errorf("Expected 3 columns after select, got %d", result.Width())
		}
	})
}

// Test Edge Cases and Error Handling
func TestEdgeCases(t *testing.T) {
	df := loadTestData(t)

	t.Run("FilterResultingInEmptyDataFrame", func(t *testing.T) {
		// Filter that should result in no rows
		result := df.Filter(polars.Col("petal.length").Gt(100))

		if result.Height() != 0 {
			t.Error("Expected empty result for impossible condition")
		}
	})

	t.Run("MultipleFilters", func(t *testing.T) {
		// Apply multiple filters in sequence
		result := df.
			Filter(polars.Col("petal.length").Gt(1)).
			Filter(polars.Col("petal.width").Gt(0)).
			Filter(polars.Col("sepal.length").Lt(10))

		if result.Height() == 0 {
			t.Error("Expected some rows after reasonable filters")
		}
	})

	t.Run("MultipleWithColumns", func(t *testing.T) {
		// Add multiple columns
		result := df.WithColumns(
			polars.Col("petal.length").AddValue(1.0).Alias("col1"),
			polars.Col("petal.width").MulValue(2.0).Alias("col2"),
			polars.Col("sepal.length").SubValue(0.5).Alias("col3"),
		)

		if result.Width() != df.Width()+3 {
			t.Errorf("Expected %d columns, got %d", df.Width()+3, result.Width())
		}
	})
}

// Benchmark tests
func BenchmarkExpressionOperations(b *testing.B) {
	df := loadTestData(&testing.T{})

	b.Run("FilterGt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = df.Filter(polars.Col("petal.length").Gt(2))
		}
	})

	b.Run("MathematicalOperations", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = df.WithColumns(
				polars.Col("petal.length").AddValue(1.0).Alias("test"),
			)
		}
	})

	b.Run("ComplexExpression", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = df.Filter(
				polars.Col("petal.length").Gt(3).And(polars.Col("petal.width").Gt(1)),
			)
		}
	})
}

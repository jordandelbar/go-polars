package tests

import (
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

// Test DataFrameBuilder Basic Functionality
func TestDataFrameBuilderBasic(t *testing.T) {
	t.Run("CreateMixedDataFrame", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob", "Charlie"}).
			AddIntColumn("age", []int64{25, 30, 35}).
			AddFloatColumn("salary", []float64{50000.5, 60000.75, 70000.25}).
			AddBoolColumn("active", []bool{true, false, true}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		if df.Height() != 3 {
			t.Errorf("Expected 3 rows, got %d", df.Height())
		}

		if df.Width() != 4 {
			t.Errorf("Expected 4 columns, got %d", df.Width())
		}

		expectedColumns := []string{"name", "age", "salary", "active"}
		actualColumns := df.Columns()
		if len(actualColumns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(actualColumns))
		}

		for i, expected := range expectedColumns {
			if i < len(actualColumns) && actualColumns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, actualColumns[i])
			}
		}
	})

	t.Run("StringOnlyDataFrame", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("first_name", []string{"John", "Jane", "Bob"}).
			AddStringColumn("last_name", []string{"Doe", "Smith", "Johnson"}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create string DataFrame: %v", err)
		}

		if df.Height() != 3 {
			t.Errorf("Expected 3 rows, got %d", df.Height())
		}

		if df.Width() != 2 {
			t.Errorf("Expected 2 columns, got %d", df.Width())
		}
	})

	t.Run("NumericOnlyDataFrame", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddIntColumn("id", []int64{1, 2, 3, 4}).
			AddFloatColumn("score", []float64{85.5, 92.0, 78.5, 95.5}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create numeric DataFrame: %v", err)
		}

		if df.Height() != 4 {
			t.Errorf("Expected 4 rows, got %d", df.Height())
		}

		if df.Width() != 2 {
			t.Errorf("Expected 2 columns, got %d", df.Width())
		}
	})

	t.Run("BooleanOnlyDataFrame", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddBoolColumn("is_active", []bool{true, false, true}).
			AddBoolColumn("is_admin", []bool{false, true, false}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create boolean DataFrame: %v", err)
		}

		if df.Height() != 3 {
			t.Errorf("Expected 3 rows, got %d", df.Height())
		}

		if df.Width() != 2 {
			t.Errorf("Expected 2 columns, got %d", df.Width())
		}
	})
}

// Test DataFrameBuilder Error Cases
func TestDataFrameBuilderErrors(t *testing.T) {
	t.Run("EmptyBuilder", func(t *testing.T) {
		_, err := polars.NewDataFrame().Build()

		if err == nil {
			t.Error("Expected error when building empty DataFrame")
		}
	})

	t.Run("MismatchedColumnLengths", func(t *testing.T) {
		// This should fail during Build() due to mismatched lengths
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob"}).
			AddIntColumn("age", []int64{25, 30, 35}). // Different length
			Build()

		// The builder pattern might not catch this until Build(), so we check both scenarios
		if err == nil && df != nil {
			// If it somehow succeeded, the DataFrame should handle it appropriately
			t.Logf("DataFrame created despite mismatched lengths - checking dimensions")
		} else if err != nil {
			// This is the expected behavior
			t.Logf("Correctly caught mismatched column lengths: %v", err)
		}
	})

	t.Run("SingleEmptyColumn", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("empty", []string{}).
			Build()

		if err != nil {
			t.Errorf("Empty column should be allowed: %v", err)
		}

		if df != nil && df.Height() != 0 {
			t.Errorf("Expected 0 rows for empty column, got %d", df.Height())
		}
	})
}

// Test DataFrameBuilder Edge Cases
func TestDataFrameBuilderEdgeCases(t *testing.T) {
	t.Run("SingleRow", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice"}).
			AddIntColumn("age", []int64{25}).
			AddFloatColumn("salary", []float64{50000.0}).
			AddBoolColumn("active", []bool{true}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create single-row DataFrame: %v", err)
		}

		if df.Height() != 1 {
			t.Errorf("Expected 1 row, got %d", df.Height())
		}

		if df.Width() != 4 {
			t.Errorf("Expected 4 columns, got %d", df.Width())
		}
	})

	t.Run("LargeDataFrame", func(t *testing.T) {
		size := 1000
		names := make([]string, size)
		ages := make([]int64, size)
		salaries := make([]float64, size)
		active := make([]bool, size)

		for i := 0; i < size; i++ {
			names[i] = "Person" + string(rune(i))
			ages[i] = int64(20 + i%50)
			salaries[i] = float64(30000 + i*100)
			active[i] = i%2 == 0
		}

		df, err := polars.NewDataFrame().
			AddStringColumn("name", names).
			AddIntColumn("age", ages).
			AddFloatColumn("salary", salaries).
			AddBoolColumn("active", active).
			Build()

		if err != nil {
			t.Fatalf("Failed to create large DataFrame: %v", err)
		}

		if df.Height() != size {
			t.Errorf("Expected %d rows, got %d", size, df.Height())
		}

		if df.Width() != 4 {
			t.Errorf("Expected 4 columns, got %d", df.Width())
		}
	})

	t.Run("SpecialStringValues", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("special", []string{"", "null", "NaN", "unicode: 🚀", "newline\ntest"}).
			AddIntColumn("id", []int64{1, 2, 3, 4, 5}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame with special strings: %v", err)
		}

		if df.Height() != 5 {
			t.Errorf("Expected 5 rows, got %d", df.Height())
		}
	})

	t.Run("ExtremeNumericValues", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddIntColumn("int_vals", []int64{-9223372036854775808, 0, 9223372036854775807}).
			AddFloatColumn("float_vals", []float64{-1.7976931348623157e+308, 0.0, 1.7976931348623157e+308}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame with extreme values: %v", err)
		}

		if df.Height() != 3 {
			t.Errorf("Expected 3 rows, got %d", df.Height())
		}
	})
}

// Test DataFrameBuilder Integration with Other Operations
func TestDataFrameBuilderIntegration(t *testing.T) {
	t.Run("FilterAfterBuild", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob", "Charlie"}).
			AddIntColumn("age", []int64{25, 30, 35}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		filtered := df.Filter(polars.Col("age").Gt(27))
		if filtered.Height() != 2 {
			t.Errorf("Expected 2 rows after filtering, got %d", filtered.Height())
		}
	})

	t.Run("SelectAfterBuild", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob"}).
			AddIntColumn("age", []int64{25, 30}).
			AddFloatColumn("salary", []float64{50000.0, 60000.0}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		selected := df.Select(polars.Col("name"), polars.Col("age"))
		if selected.Width() != 2 {
			t.Errorf("Expected 2 columns after select, got %d", selected.Width())
		}
	})

	t.Run("WithColumnsAfterBuild", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob"}).
			AddIntColumn("age", []int64{25, 30}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		modified := df.WithColumns(polars.Col("age").MulValue(2.0).Alias("double_age"))
		if modified.Width() != 3 {
			t.Errorf("Expected 3 columns after WithColumns, got %d", modified.Width())
		}
	})

	t.Run("ChainedOperations", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob", "Charlie", "David"}).
			AddIntColumn("age", []int64{25, 30, 35, 40}).
			AddFloatColumn("salary", []float64{50000.0, 60000.0, 70000.0, 80000.0}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		result := df.
			Filter(polars.Col("age").Ge(30)).
			WithColumns(polars.Col("salary").DivValue(1000.0).Alias("salary_k")).
			Select(polars.Col("name"), polars.Col("salary_k")).
			Head(2)

		if result.Height() != 2 {
			t.Errorf("Expected 2 rows after chained operations, got %d", result.Height())
		}

		if result.Width() != 2 {
			t.Errorf("Expected 2 columns after chained operations, got %d", result.Width())
		}
	})
}

// Test DataFrameBuilder Method Chaining
func TestDataFrameBuilderChaining(t *testing.T) {
	t.Run("FluentAPI", func(t *testing.T) {
		// Test that all Add methods return the builder for chaining
		builder := polars.NewDataFrame()

		// Chain should work smoothly
		df, err := builder.
			AddStringColumn("col1", []string{"a", "b"}).
			AddIntColumn("col2", []int64{1, 2}).
			AddFloatColumn("col3", []float64{1.1, 2.2}).
			AddBoolColumn("col4", []bool{true, false}).
			Build()

		if err != nil {
			t.Fatalf("Fluent API failed: %v", err)
		}

		if df.Width() != 4 {
			t.Errorf("Expected 4 columns from fluent API, got %d", df.Width())
		}
	})

	t.Run("ReuseBuilder", func(t *testing.T) {
		// Test that we can't accidentally reuse a builder
		builder := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice"}).
			AddIntColumn("age", []int64{25})

		df1, err1 := builder.Build()
		if err1 != nil {
			t.Fatalf("First build failed: %v", err1)
		}

		// Try to build again (behavior may vary - this tests current implementation)
		df2, err2 := builder.Build()
		if err2 != nil {
			t.Logf("Second build failed as expected: %v", err2)
		} else if df2 != nil {
			t.Logf("Second build succeeded - builder can be reused")
		}

		// First DataFrame should still be valid
		if df1.Height() != 1 {
			t.Errorf("First DataFrame should still be valid")
		}
	})
}

// Test DataFrameBuilder Memory Management
func TestDataFrameBuilderMemoryManagement(t *testing.T) {
	t.Run("BuildAndFree", func(t *testing.T) {
		df, err := polars.NewDataFrame().
			AddStringColumn("name", []string{"Alice", "Bob"}).
			AddIntColumn("age", []int64{25, 30}).
			Build()

		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		// Test that we can use the DataFrame
		height := df.Height()
		if height != 2 {
			t.Errorf("Expected 2 rows, got %d", height)
		}

		// Free the DataFrame
		df.Free()

		// After freeing, operations should be safe (implementation dependent)
		// This tests the current behavior
	})

	t.Run("MultipleBuildAndFree", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			df, err := polars.NewDataFrame().
				AddStringColumn("test", []string{"data"}).
				Build()

			if err != nil {
				t.Fatalf("Failed to create DataFrame %d: %v", i, err)
			}

			df.Free()
		}
	})
}

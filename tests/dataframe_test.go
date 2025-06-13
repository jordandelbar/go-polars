package tests

import (
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

// Test basic DataFrame operations
func TestDataFrameBasicOperations(t *testing.T) {
	df := loadTestData(t)

	t.Run("Height", func(t *testing.T) {
		height := df.Height()
		if height != 150 {
			t.Errorf("Expected height 150, got %d", height)
		}
	})

	t.Run("Width", func(t *testing.T) {
		width := df.Width()
		if width != 5 {
			t.Errorf("Expected width 5, got %d", width)
		}
	})

	t.Run("Columns", func(t *testing.T) {
		columns := df.Columns()
		expectedColumns := []string{"sepal.length", "sepal.width", "petal.length", "petal.width", "variety"}

		if len(columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
		}

		for i, expected := range expectedColumns {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})

	t.Run("Head", func(t *testing.T) {
		head5 := df.Head(5)
		if head5.Height() != 5 {
			t.Errorf("Expected head(5) to have 5 rows, got %d", head5.Height())
		}

		if head5.Width() != df.Width() {
			t.Errorf("Expected head to have same width as original, got %d vs %d", head5.Width(), df.Width())
		}

		// Test edge case: head larger than dataframe
		headLarge := df.Head(200)
		if headLarge.Height() > df.Height() {
			t.Errorf("Head should not return more rows than the original dataframe")
		}
	})

	t.Run("String", func(t *testing.T) {
		str := df.String()
		if str == "" {
			t.Error("DataFrame string representation should not be empty")
		}

		if str == "<nil DataFrame>" {
			t.Error("DataFrame should not be nil")
		}
	})
}

// Test DataFrame filtering
func TestDataFrameFiltering(t *testing.T) {
	df := loadTestData(t)

	t.Run("BasicFilter", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Gt(1))

		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length > 1")
		}

		if filtered.Height() > df.Height() {
			t.Error("Filtered result should not have more rows than original")
		}

		if filtered.Width() != df.Width() {
			t.Error("Filtered result should have same width as original")
		}
	})

	t.Run("FilterChaining", func(t *testing.T) {
		result := df.
			Filter(polars.Col("petal.length").Gt(1)).
			Filter(polars.Col("sepal.length").Lt(8))

		if result.Height() == 0 {
			t.Error("Expected some rows after chained filters")
		}

		if result.Height() > df.Height() {
			t.Error("Chained filters should not increase row count")
		}
	})
}

// Test DataFrame column selection
func TestDataFrameSelection(t *testing.T) {
	df := loadTestData(t)

	t.Run("SelectSingleColumn", func(t *testing.T) {
		selected := df.Select(polars.Col("petal.length"))

		if selected.Width() != 1 {
			t.Errorf("Expected 1 column after selecting single column, got %d", selected.Width())
		}

		if selected.Height() != df.Height() {
			t.Error("Selection should not change row count")
		}

		columns := selected.Columns()
		if len(columns) != 1 || columns[0] != "petal.length" {
			t.Errorf("Expected column 'petal.length', got %v", columns)
		}
	})

	t.Run("SelectMultipleColumns", func(t *testing.T) {
		selected := df.Select(polars.Col("petal.length"), polars.Col("sepal.width"))

		if selected.Width() != 2 {
			t.Errorf("Expected 2 columns after selecting two columns, got %d", selected.Width())
		}

		if selected.Height() != df.Height() {
			t.Error("Selection should not change row count")
		}

		columns := selected.Columns()
		expectedCols := []string{"petal.length", "sepal.width"}

		if len(columns) != len(expectedCols) {
			t.Errorf("Expected %d columns, got %d", len(expectedCols), len(columns))
		}

		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})

	t.Run("SelectAllColumns", func(t *testing.T) {
		selected := df.Select(
			polars.Col("sepal.length"),
			polars.Col("sepal.width"),
			polars.Col("petal.length"),
			polars.Col("petal.width"),
			polars.Col("variety"),
		)

		if selected.Width() != df.Width() {
			t.Error("Selecting all columns should preserve width")
		}

		if selected.Height() != df.Height() {
			t.Error("Selecting all columns should preserve height")
		}
	})
}

// Test DataFrame column addition
func TestDataFrameWithColumns(t *testing.T) {
	df := loadTestData(t)

	t.Run("AddSingleColumn", func(t *testing.T) {
		result := df.WithColumns(polars.Lit("test_value").Alias("test_column"))

		if result.Width() != df.Width()+1 {
			t.Errorf("Expected width to increase by 1, got %d vs %d", result.Width(), df.Width())
		}

		if result.Height() != df.Height() {
			t.Error("Adding column should not change row count")
		}

		columns := result.Columns()
		found := false
		for _, col := range columns {
			if col == "test_column" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find 'test_column' in result")
		}
	})

	t.Run("AddMultipleColumns", func(t *testing.T) {
		result := df.WithColumns(
			polars.Lit("value1").Alias("col1"),
			polars.Lit(123).Alias("col2"),
			polars.Lit(45.6).Alias("col3"),
		)

		if result.Width() != df.Width()+3 {
			t.Errorf("Expected width to increase by 3, got %d vs %d", result.Width(), df.Width())
		}

		if result.Height() != df.Height() {
			t.Error("Adding columns should not change row count")
		}

		columns := result.Columns()
		expectedNewCols := []string{"col1", "col2", "col3"}

		for _, expected := range expectedNewCols {
			found := false
			for _, col := range columns {
				if col == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected to find column '%s' in result", expected)
			}
		}
	})

	t.Run("ReplaceExistingColumn", func(t *testing.T) {
		// Replace an existing column by using the same name
		result := df.WithColumns(polars.Lit("replaced").Alias("variety"))

		if result.Width() != df.Width() {
			t.Error("Replacing existing column should not change width")
		}

		if result.Height() != df.Height() {
			t.Error("Replacing existing column should not change height")
		}
	})
}

// Test Literal expressions
func TestLiteralExpressions(t *testing.T) {
	df := loadTestData(t)

	t.Run("StringLiteral", func(t *testing.T) {
		result := df.WithColumns(polars.Lit("hello").Alias("string_col"))

		columns := result.Columns()
		found := false
		for _, col := range columns {
			if col == "string_col" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find string literal column")
		}
	})

	t.Run("IntLiteral", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(42).Alias("int_col"))

		if result.Width() != df.Width()+1 {
			t.Error("Expected width to increase by 1")
		}
	})

	t.Run("FloatLiteral", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(3.14).Alias("float_col"))

		if result.Width() != df.Width()+1 {
			t.Error("Expected width to increase by 1")
		}
	})

	t.Run("BoolLiteral", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(true).Alias("bool_col"))

		if result.Width() != df.Width()+1 {
			t.Error("Expected width to increase by 1")
		}
	})
}

// Test DataFrame memory management
func TestDataFrameMemoryManagement(t *testing.T) {
	df := loadTestData(t)

	t.Run("Free", func(t *testing.T) {
		// Create a copy to test freeing
		filtered := df.Filter(polars.Col("petal.length").Gt(1))

		// This should not panic
		filtered.Free()

		// After freeing, string representation should indicate nil
		str := filtered.String()
		if str != "<nil DataFrame>" {
			t.Errorf("Expected '<nil DataFrame>' after free, got: %s", str)
		}
	})
}

// Test combined operations
func TestCombinedOperations(t *testing.T) {
	df := loadTestData(t)

	t.Run("FilterSelectHead", func(t *testing.T) {
		result := df.
			Filter(polars.Col("petal.length").Gt(2)).
			Select(polars.Col("variety"), polars.Col("petal.length")).
			Head(10)

		if result.Width() != 2 {
			t.Errorf("Expected 2 columns, got %d", result.Width())
		}

		if result.Height() > 10 {
			t.Errorf("Expected at most 10 rows, got %d", result.Height())
		}

		if result.Height() == 0 {
			t.Error("Expected some rows in result")
		}
	})

	t.Run("WithColumnsFilterSelect", func(t *testing.T) {
		result := df.
			WithColumns(polars.Col("petal.length").MulValue(2.0).Alias("doubled_petal")).
			Filter(polars.Col("doubled_petal").Lt(10)).
			Select(polars.Col("variety"), polars.Col("petal.length"), polars.Col("doubled_petal"))

		if result.Width() != 3 {
			t.Errorf("Expected 3 columns, got %d", result.Width())
		}

		if result.Height() == 0 {
			t.Error("Expected some rows in result")
		}

		columns := result.Columns()
		expectedCols := []string{"variety", "petal.length", "doubled_petal"}

		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})
}

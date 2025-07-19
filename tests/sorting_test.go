package tests

import (
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

func TestDataFrameSorting(t *testing.T) {
	df := loadTestData(t)

	t.Run("SortSingleColumnAscending", func(t *testing.T) {
		sorted := df.Sort("petal.length")

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}

		// Check that we have a valid DataFrame
		if sorted.String() == "" {
			t.Error("Expected non-empty string representation")
		}
	})

	t.Run("SortSingleColumnDescending", func(t *testing.T) {
		sorted := df.SortBy([]string{"petal.length"}, []bool{true})

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}

		// Check that we have a valid DataFrame
		if sorted.String() == "" {
			t.Error("Expected non-empty string representation")
		}
	})

	t.Run("SortMultipleColumnsAscending", func(t *testing.T) {
		sorted := df.Sort("variety", "petal.length")

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}

		// Check that we have a valid DataFrame
		if sorted.String() == "" {
			t.Error("Expected non-empty string representation")
		}
	})

	t.Run("SortMultipleColumnsMixedOrder", func(t *testing.T) {
		sorted := df.SortBy(
			[]string{"variety", "petal.length"},
			[]bool{false, true}, // variety ascending, petal.length descending
		)

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}

		// Check that we have a valid DataFrame
		if sorted.String() == "" {
			t.Error("Expected non-empty string representation")
		}
	})

	t.Run("SortByExpressions", func(t *testing.T) {
		sorted := df.SortByExprs(
			[]polars.Expr{polars.Col("petal.length"), polars.Col("sepal.width")},
			[]bool{false, true}, // petal.length ascending, sepal.width descending
		)

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}

		// Check that we have a valid DataFrame
		if sorted.String() == "" {
			t.Error("Expected non-empty string representation")
		}
	})

	t.Run("SortEmptyColumnList", func(t *testing.T) {
		// Sorting with no columns should return the original DataFrame
		sorted := df.Sort()

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}
	})

	t.Run("SortInvalidColumn", func(t *testing.T) {
		// Sorting by a non-existent column should handle the error gracefully
		sorted := df.Sort("non_existent_column")

		// The result should be an empty DataFrame indicating an error
		if sorted.Height() != 0 || sorted.Width() != 0 {
			t.Error("Expected error when sorting by non-existent column")
		}
	})

	t.Run("SortByMismatchedArrays", func(t *testing.T) {
		// Test with mismatched array lengths
		sorted := df.SortBy(
			[]string{"petal.length", "sepal.width"},
			[]bool{true}, // Only one boolean for two columns
		)

		// Should return an empty DataFrame due to error
		if sorted.Height() != 0 || sorted.Width() != 0 {
			t.Error("Expected error when arrays have mismatched lengths")
		}
	})

	t.Run("SortByExprsMismatchedArrays", func(t *testing.T) {
		// Test with mismatched array lengths for expressions
		sorted := df.SortByExprs(
			[]polars.Expr{polars.Col("petal.length"), polars.Col("sepal.width")},
			[]bool{true}, // Only one boolean for two expressions
		)

		// Should return an empty DataFrame due to error
		if sorted.Height() != 0 || sorted.Width() != 0 {
			t.Error("Expected error when arrays have mismatched lengths")
		}
	})
}

func TestSortingChaining(t *testing.T) {
	df := loadTestData(t)

	t.Run("SortAfterFilter", func(t *testing.T) {
		result := df.
			Filter(polars.Col("petal.length").Gt(1)).
			Sort("sepal.length")

		if result.Height() == 0 {
			t.Error("Expected some rows after filtering and sorting")
		}

		if result.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), result.Width())
		}
	})

	t.Run("SortThenSelect", func(t *testing.T) {
		result := df.
			Sort("petal.length").
			Select(polars.Col("variety"), polars.Col("petal.length"))

		if result.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), result.Height())
		}

		if result.Width() != 2 {
			t.Errorf("Expected width to be 2, got %d", result.Width())
		}
	})

	t.Run("SortThenHead", func(t *testing.T) {
		result := df.
			Sort("petal.length").
			Head(10)

		if result.Height() != 10 {
			t.Errorf("Expected height to be 10, got %d", result.Height())
		}

		if result.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), result.Width())
		}
	})

	t.Run("ComplexSortingChain", func(t *testing.T) {
		result := df.
			Filter(polars.Col("petal.length").Gt(1)).
			WithColumns(polars.Col("petal.length").MulValue(2.0).Alias("doubled_petal")).
			SortBy([]string{"variety", "doubled_petal"}, []bool{false, true}).
			Select(polars.Col("variety"), polars.Col("petal.length"), polars.Col("doubled_petal")).
			Head(20)

		if result.Height() > 20 {
			t.Errorf("Expected height to be at most 20, got %d", result.Height())
		}

		if result.Width() != 3 {
			t.Errorf("Expected width to be 3, got %d", result.Width())
		}

		// Verify the doubled_petal column exists
		columns := result.Columns()
		found := false
		for _, col := range columns {
			if col == "doubled_petal" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected doubled_petal column to exist")
		}
	})
}

func TestSortingWithNullHandling(t *testing.T) {
	df := loadTestData(t)

	t.Run("SortWithPotentialNulls", func(t *testing.T) {
		// Even if there are no nulls in our test data,
		// the sorting should handle it gracefully
		sorted := df.Sort("petal.length")

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		// Check that sorting doesn't crash with potential null values
		if sorted.String() == "" {
			t.Error("Expected non-empty string representation")
		}
	})
}

func TestSortingPerformance(t *testing.T) {
	df := loadTestData(t)

	t.Run("SortPerformanceBaseline", func(t *testing.T) {
		// This test ensures sorting completes in reasonable time
		// Not testing specific timing, just that it doesn't hang
		sorted := df.Sort("petal.length", "sepal.width", "variety")

		if sorted.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), sorted.Height())
		}

		if sorted.Width() != df.Width() {
			t.Errorf("Expected width to remain %d, got %d", df.Width(), sorted.Width())
		}
	})

	t.Run("MultipleSortsPerformance", func(t *testing.T) {
		// Test multiple sorts in sequence
		result := df
		for i := 0; i < 5; i++ {
			result = result.Sort("petal.length")
		}

		if result.Height() != df.Height() {
			t.Errorf("Expected height to remain %d, got %d", df.Height(), result.Height())
		}
	})
}

func TestSortingMemoryManagement(t *testing.T) {
	df := loadTestData(t)

	t.Run("SortMemoryHandling", func(t *testing.T) {
		sorted := df.Sort("petal.length")

		// Test that we can free the sorted DataFrame
		sorted.Free()

		// After freeing, string representation should indicate nil
		str := sorted.String()
		if str != "<nil DataFrame>" {
			t.Errorf("Expected '<nil DataFrame>' after freeing, got '%s'", str)
		}
	})

	t.Run("MultipleSortsFree", func(t *testing.T) {
		// Create multiple sorted DataFrames and free them
		sorted1 := df.Sort("petal.length")
		sorted2 := df.SortBy([]string{"variety"}, []bool{true})
		sorted3 := df.SortByExprs([]polars.Expr{polars.Col("sepal.width")}, []bool{false})

		// Free all of them - should not cause memory issues
		sorted1.Free()
		sorted2.Free()
		sorted3.Free()

		// All should show nil state
		if sorted1.String() != "<nil DataFrame>" {
			t.Error("Expected sorted1 to be nil after freeing")
		}
		if sorted2.String() != "<nil DataFrame>" {
			t.Error("Expected sorted2 to be nil after freeing")
		}
		if sorted3.String() != "<nil DataFrame>" {
			t.Error("Expected sorted3 to be nil after freeing")
		}
	})
}

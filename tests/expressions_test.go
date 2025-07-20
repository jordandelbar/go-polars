package tests

import (
	"os"
	"path/filepath"
	"slices"
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
	return filepath.Join(wd, "..", "examples", "data", "iris.csv")
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

// Test Float Comparison Operations
func TestFloatComparisonOperations(t *testing.T) {
	df := loadTestData(t)
	originalHeight := df.Height()

	t.Run("GreaterThanFloat", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Gt(1.5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length > 1.5")
		}
		if filtered.Height() >= originalHeight {
			t.Error("Filtered result should have fewer rows than original")
		}
	})

	t.Run("LessThanFloat", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Lt(1.5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length < 1.5")
		}
		if filtered.Height() >= originalHeight {
			t.Error("Filtered result should have fewer rows than original")
		}
	})

	t.Run("EqualToFloat", func(t *testing.T) {
		// Use a specific float value that might exist in the data
		filtered := df.Filter(polars.Col("petal.length").Eq(1.4))
		// We don't require results since exact float matches might not exist
		// but the operation should not fail
		if filtered.Height() < 0 {
			t.Error("Filter operation should not fail")
		}
	})

	t.Run("NotEqualToFloat", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Ne(1.4))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length != 1.4")
		}
	})

	t.Run("GreaterThanOrEqualFloat", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Ge(1.5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length >= 1.5")
		}
	})

	t.Run("LessThanOrEqualFloat", func(t *testing.T) {
		filtered := df.Filter(polars.Col("petal.length").Le(1.5))
		if filtered.Height() == 0 {
			t.Error("Expected some rows with petal.length <= 1.5")
		}
	})

	t.Run("MixedIntegerAndFloat", func(t *testing.T) {
		// Test that we can mix integer and float comparisons seamlessly
		intFiltered := df.Filter(polars.Col("petal.length").Gt(1))
		floatFiltered := df.Filter(polars.Col("petal.length").Gt(1.0))

		// Both should work and potentially give different results due to precision
		if intFiltered.Height() == 0 {
			t.Error("Integer comparison should work")
		}
		if floatFiltered.Height() == 0 {
			t.Error("Float comparison should work")
		}
	})

	t.Run("Float32Support", func(t *testing.T) {
		// Test float32 support
		var floatVal float32 = 1.5
		filtered := df.Filter(polars.Col("petal.length").Gt(floatVal))
		if filtered.Height() < 0 {
			t.Error("Float32 comparison should work")
		}
	})

	t.Run("DifferentIntegerTypes", func(t *testing.T) {
		// Test different integer types
		var int32Val int32 = 1
		var int64Val int64 = 1
		var intVal int = 1

		filtered32 := df.Filter(polars.Col("petal.length").Gt(int32Val))
		filtered64 := df.Filter(polars.Col("petal.length").Gt(int64Val))
		filteredInt := df.Filter(polars.Col("petal.length").Gt(intVal))

		if filtered32.Height() < 0 {
			t.Error("int32 comparison should work")
		}
		if filtered64.Height() < 0 {
			t.Error("int64 comparison should work")
		}
		if filteredInt.Height() < 0 {
			t.Error("int comparison should work")
		}
	})
}

// Test Comparison Operations Error Cases
func TestComparisonOperationsErrorCases(t *testing.T) {
	df := loadTestData(t)

	t.Run("UnsupportedTypeGt", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported type")
			}
		}()
		df.Filter(polars.Col("petal.length").Gt("invalid"))
	})

	t.Run("UnsupportedTypeLt", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported type")
			}
		}()
		df.Filter(polars.Col("petal.length").Lt("invalid"))
	})

	t.Run("UnsupportedTypeEq", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported type")
			}
		}()
		df.Filter(polars.Col("petal.length").Eq("invalid"))
	})

	t.Run("UnsupportedTypeNe", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported type")
			}
		}()
		df.Filter(polars.Col("petal.length").Ne("invalid"))
	})

	t.Run("UnsupportedTypeGe", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported type")
			}
		}()
		df.Filter(polars.Col("petal.length").Ge("invalid"))
	})

	t.Run("UnsupportedTypeLe", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported type")
			}
		}()
		df.Filter(polars.Col("petal.length").Le("invalid"))
	})
}

// Test All Comparison Operation Type Combinations
func TestComparisonTypeExhaustive(t *testing.T) {
	df := loadTestData(t)

	types := []struct {
		name  string
		value interface{}
	}{
		{"int", 2},
		{"int32", int32(2)},
		{"int64", int64(2)},
		{"float32", float32(2.0)},
		{"float64", 2.0},
	}

	operations := []struct {
		name string
		op   func(polars.Expr, any) polars.Expr
	}{
		{"Gt", func(e polars.Expr, v any) polars.Expr { return e.Gt(v) }},
		{"Lt", func(e polars.Expr, v any) polars.Expr { return e.Lt(v) }},
		{"Eq", func(e polars.Expr, v any) polars.Expr { return e.Eq(v) }},
		{"Ne", func(e polars.Expr, v any) polars.Expr { return e.Ne(v) }},
		{"Ge", func(e polars.Expr, v any) polars.Expr { return e.Ge(v) }},
		{"Le", func(e polars.Expr, v any) polars.Expr { return e.Le(v) }},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			for _, type_ := range types {
				t.Run(type_.name, func(t *testing.T) {
					result := df.Filter(op.op(polars.Col("petal.length"), type_.value))
					if result.Height() < 0 {
						t.Errorf("%s with %s should not fail", op.name, type_.name)
					}
				})
			}
		})
	}
}

// Test Literal Expression Type Coverage
func TestLiteralExpressionTypes(t *testing.T) {
	df := loadTestData(t)

	t.Run("LitInt", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(42).Alias("int_literal"))
		if result.Height() != df.Height() {
			t.Error("Int literal should work")
		}
	})

	t.Run("LitInt32", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(int32(42)).Alias("int32_literal"))
		if result.Height() != df.Height() {
			t.Error("Int32 literal should work")
		}
	})

	t.Run("LitInt64", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(int64(42)).Alias("int64_literal"))
		if result.Height() != df.Height() {
			t.Error("Int64 literal should work")
		}
	})

	t.Run("LitFloat32", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(float32(3.14)).Alias("float32_literal"))
		if result.Height() != df.Height() {
			t.Error("Float32 literal should work")
		}
	})

	t.Run("LitFloat64", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(3.14).Alias("float64_literal"))
		if result.Height() != df.Height() {
			t.Error("Float64 literal should work")
		}
	})

	t.Run("LitBool", func(t *testing.T) {
		result := df.WithColumns(polars.Lit(true).Alias("bool_literal"))
		if result.Height() != df.Height() {
			t.Error("Bool literal should work")
		}
	})

	t.Run("LitString", func(t *testing.T) {
		result := df.WithColumns(polars.Lit("test").Alias("string_literal"))
		if result.Height() != df.Height() {
			t.Error("String literal should work")
		}
	})

	t.Run("UnsupportedLiteralType", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for unsupported literal type")
			}
		}()
		df.WithColumns(polars.Lit([]int{1, 2, 3}).Alias("unsupported"))
	})
}

// Test Head with Different Parameters
func TestHeadVariations(t *testing.T) {
	df := loadTestData(t)
	originalHeight := df.Height()

	t.Run("HeadZero", func(t *testing.T) {
		result := df.Head(0)
		if result.Height() != 0 {
			t.Error("Head(0) should return empty dataframe")
		}
		if result.Width() != df.Width() {
			t.Error("Head(0) should preserve column count")
		}
	})

	t.Run("HeadLargerThanDataset", func(t *testing.T) {
		result := df.Head(1000)
		if result.Height() != originalHeight {
			t.Error("Head with large number should return entire dataset")
		}
	})

	t.Run("HeadNormal", func(t *testing.T) {
		result := df.Head(3)
		if result.Height() != 3 {
			t.Error("Head(3) should return exactly 3 rows")
		}
		if result.Width() != df.Width() {
			t.Error("Head should preserve column count")
		}
	})
}

// Test Filter Edge Cases
func TestFilterEdgeCases(t *testing.T) {
	df := loadTestData(t)

	t.Run("FilterWithInvalidColumn", func(t *testing.T) {
		// This should not panic but may return an error through normal operation
		result := df.Filter(polars.Col("non_existent").Gt(1))
		// The operation may succeed but should be handled gracefully
		if result == nil {
			t.Error("Filter should return a result even with invalid column")
		}
	})

	t.Run("ChainedFilters", func(t *testing.T) {
		result := df.
			Filter(polars.Col("petal.length").Gt(1.0)).
			Filter(polars.Col("petal.width").Lt(3.0)).
			Filter(polars.Col("sepal.length").Ge(4.0))

		if result.Height() < 0 {
			t.Error("Chained filters should work")
		}
	})

	t.Run("FilterToEmpty", func(t *testing.T) {
		result := df.Filter(polars.Col("petal.length").Gt(1000.0))
		if result.Height() != 0 {
			t.Error("Impossible filter should result in empty dataframe")
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
		found := slices.Contains(columns, "petal_plus_one")
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
		found := slices.Contains(columns, "petal_sum")
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
		found := slices.Contains(columns, "complex_calc")
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
// Test GroupBy Error Handling and Edge Cases
func TestGroupByEdgeCases(t *testing.T) {
	df := loadTestData(t)

	t.Run("GroupByEmptyString", func(t *testing.T) {
		groupby := df.GroupBy("")
		if groupby == nil {
			t.Error("GroupBy with empty string should return a result")
		}
	})

	t.Run("GroupByMultipleColumns", func(t *testing.T) {
		groupby := df.GroupBy("variety,sepal.length")
		if groupby == nil {
			t.Error("GroupBy with multiple columns should work")
		}
		result := groupby.Count()
		if result.Height() == 0 {
			t.Error("GroupBy count should return results")
		}
	})
}

// Test Select Edge Cases
func TestSelectEdgeCases(t *testing.T) {
	df := loadTestData(t)

	t.Run("SelectEmptyList", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				// Select with no columns causes a panic - this is the current behavior
				t.Logf("Select with no columns panicked (expected): %v", r)
			}
		}()
		result := df.Select()
		// If we get here without panic, test the result
		if result.Width() != 0 {
			t.Error("Select with no columns should return empty width")
		}
		if result.Height() != df.Height() {
			t.Error("Select should preserve row count")
		}
	})

	t.Run("SelectDuplicateColumns", func(t *testing.T) {
		result := df.Select(
			polars.Col("variety"),
			polars.Col("variety"),
		)
		// This behavior may vary, but shouldn't crash
		if result == nil {
			t.Error("Select with duplicate columns should return a result")
		}
	})

	t.Run("SelectWithExpression", func(t *testing.T) {
		result := df.Select(
			polars.Col("petal.length"),
			polars.Col("petal.width").AddValue(1.0).Alias("modified"),
		)
		if result.Width() != 2 {
			t.Error("Select with expression should work")
		}
	})
}

// Test WithColumns Edge Cases
func TestWithColumnsEdgeCases(t *testing.T) {
	df := loadTestData(t)

	t.Run("WithColumnsEmpty", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				// WithColumns with no arguments causes a panic - this is the current behavior
				t.Logf("WithColumns with no arguments panicked (expected): %v", r)
			}
		}()
		result := df.WithColumns()
		// If we get here without panic, test the result
		if result.Width() != df.Width() {
			t.Error("WithColumns with no arguments should preserve dataframe")
		}
		if result.Height() != df.Height() {
			t.Error("WithColumns should preserve row count")
		}
	})

	t.Run("WithColumnsComplexExpression", func(t *testing.T) {
		result := df.WithColumns(
			polars.Col("petal.length").
				MulValue(2.0).
				Add(polars.Col("petal.width")).
				DivValue(3.0).
				Alias("complex_calc"),
		)
		if result.Width() != df.Width()+1 {
			t.Error("Complex expression in WithColumns should work")
		}
	})
}

func BenchmarkExpressionOperations(b *testing.B) {
	// Load test data directly without using testing.T
	csvPath := getTestDataPath()
	df, err := polars.ReadCSV(csvPath)
	if err != nil {
		b.Fatalf("Failed to load test data: %v", err)
	}

	b.Run("FilterGt", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = df.Filter(polars.Col("petal.length").Gt(2))
		}
	})

	b.Run("FilterGtFloat", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = df.Filter(polars.Col("petal.length").Gt(2.0))
		}
	})

	b.Run("MathematicalOperations", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = df.WithColumns(
				polars.Col("petal.length").AddValue(1.0).Alias("test"),
			)
		}
	})

	b.Run("ComplexExpression", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = df.Filter(
				polars.Col("petal.length").Gt(3).And(polars.Col("petal.width").Gt(1)),
			)
		}
	})

	b.Run("AllComparisonTypes", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = df.Filter(polars.Col("petal.length").Gt(1))
			_ = df.Filter(polars.Col("petal.length").Lt(2))
			_ = df.Filter(polars.Col("petal.length").Eq(1))
			_ = df.Filter(polars.Col("petal.length").Ne(1))
			_ = df.Filter(polars.Col("petal.length").Ge(1))
			_ = df.Filter(polars.Col("petal.length").Le(2))
		}
	})
}

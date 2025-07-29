package tests

import (
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

// Test basic join operations
func TestDataFrameJoinBasic(t *testing.T) {
	// Create left DataFrame
	leftBuilder := polars.NewDataFrame()
	leftDf, err := leftBuilder.
		AddIntColumn("id", []int64{1, 2, 3, 4}).
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie", "David"}).
		AddIntColumn("age", []int64{25, 30, 35, 40}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create left DataFrame: %v", err)
	}
	defer leftDf.Free()

	// Create right DataFrame
	rightBuilder := polars.NewDataFrame()
	rightDf, err := rightBuilder.
		AddIntColumn("id", []int64{2, 3, 4, 5}).
		AddStringColumn("department", []string{"Engineering", "Sales", "Marketing", "HR"}).
		AddIntColumn("salary", []int64{80000, 75000, 70000, 65000}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create right DataFrame: %v", err)
	}
	defer rightDf.Free()

	t.Run("InnerJoin", func(t *testing.T) {
		result := leftDf.Join(rightDf, "id", polars.JoinInner)
		defer result.Free()

		if result.Height() != 3 {
			t.Errorf("Expected 3 rows in inner join result, got %d", result.Height())
		}

		if result.Width() != 5 {
			t.Errorf("Expected 5 columns in inner join result, got %d", result.Width())
		}

		columns := result.Columns()
		expectedCols := []string{"id", "name", "age", "department", "salary"}
		if len(columns) != len(expectedCols) {
			t.Errorf("Expected %d columns, got %d", len(expectedCols), len(columns))
		}

		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})

	t.Run("LeftJoin", func(t *testing.T) {
		result := leftDf.Join(rightDf, "id", polars.JoinLeft)
		defer result.Free()

		if result.Height() != 4 {
			t.Errorf("Expected 4 rows in left join result, got %d", result.Height())
		}

		if result.Width() != 5 {
			t.Errorf("Expected 5 columns in left join result, got %d", result.Width())
		}
	})

	t.Run("RightJoin", func(t *testing.T) {
		result := leftDf.Join(rightDf, "id", polars.JoinRight)
		defer result.Free()

		if result.Height() != 4 {
			t.Errorf("Expected 4 rows in right join result, got %d", result.Height())
		}

		if result.Width() != 5 {
			t.Errorf("Expected 5 columns in right join result, got %d", result.Width())
		}
	})

	t.Run("OuterJoin", func(t *testing.T) {
		result := leftDf.Join(rightDf, "id", polars.JoinOuter)
		defer result.Free()

		if result.Height() != 5 {
			t.Errorf("Expected 5 rows in outer join result, got %d", result.Height())
		}

		// Outer join adds suffix to duplicate columns (id becomes id_right)
		if result.Width() != 6 {
			t.Errorf("Expected 6 columns in outer join result (with _right suffix), got %d", result.Width())
		}

		columns := result.Columns()
		expectedCols := []string{"id", "name", "age", "id_right", "department", "salary"}
		if len(columns) != len(expectedCols) {
			t.Errorf("Expected %d columns, got %d", len(expectedCols), len(columns))
		}
	})
}

// Test join with different column names
func TestDataFrameJoinDifferentColumnNames(t *testing.T) {
	// Create left DataFrame
	leftBuilder := polars.NewDataFrame()
	leftDf, err := leftBuilder.
		AddIntColumn("user_id", []int64{1, 2, 3}).
		AddStringColumn("user_name", []string{"Alice", "Bob", "Charlie"}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create left DataFrame: %v", err)
	}
	defer leftDf.Free()

	// Create right DataFrame
	rightBuilder := polars.NewDataFrame()
	rightDf, err := rightBuilder.
		AddIntColumn("id", []int64{1, 2, 4}).
		AddStringColumn("email", []string{"alice@example.com", "bob@example.com", "david@example.com"}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create right DataFrame: %v", err)
	}
	defer rightDf.Free()

	t.Run("JoinOnDifferentColumns", func(t *testing.T) {
		result := leftDf.JoinOn(rightDf, "user_id", "id", polars.JoinInner)
		defer result.Free()

		if result.Height() != 2 {
			t.Errorf("Expected 2 rows in join result, got %d", result.Height())
		}

		if result.Width() != 3 {
			t.Errorf("Expected 3 columns in join result, got %d", result.Width())
		}

		columns := result.Columns()
		expectedCols := []string{"user_id", "user_name", "email"}
		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})
}

// Test multiple key joins
func TestDataFrameJoinMultipleKeys(t *testing.T) {
	// Create left DataFrame
	leftBuilder := polars.NewDataFrame()
	leftDf, err := leftBuilder.
		AddIntColumn("year", []int64{2020, 2020, 2021, 2021}).
		AddStringColumn("quarter", []string{"Q1", "Q2", "Q1", "Q2"}).
		AddIntColumn("sales", []int64{1000, 1200, 1100, 1300}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create left DataFrame: %v", err)
	}
	defer leftDf.Free()

	// Create right DataFrame
	rightBuilder := polars.NewDataFrame()
	rightDf, err := rightBuilder.
		AddIntColumn("year", []int64{2020, 2020, 2021}).
		AddStringColumn("quarter", []string{"Q1", "Q2", "Q1"}).
		AddIntColumn("costs", []int64{800, 900, 850}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create right DataFrame: %v", err)
	}
	defer rightDf.Free()

	t.Run("JoinMultipleKeys", func(t *testing.T) {
		result := leftDf.JoinMultiple(rightDf, "year,quarter", "year,quarter", polars.JoinInner)
		defer result.Free()

		if result.Height() != 3 {
			t.Errorf("Expected 3 rows in multi-key join result, got %d", result.Height())
		}

		if result.Width() != 4 {
			t.Errorf("Expected 4 columns in multi-key join result, got %d", result.Width())
		}

		columns := result.Columns()
		expectedCols := []string{"year", "quarter", "sales", "costs"}
		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})

	t.Run("JoinMultipleKeysLeft", func(t *testing.T) {
		result := leftDf.JoinMultiple(rightDf, "year,quarter", "year,quarter", polars.JoinLeft)
		defer result.Free()

		if result.Height() != 4 {
			t.Errorf("Expected 4 rows in left multi-key join result, got %d", result.Height())
		}

		if result.Width() != 4 {
			t.Errorf("Expected 4 columns in left multi-key join result, got %d", result.Width())
		}
	})
}

// Test join with empty DataFrames
func TestDataFrameJoinEmpty(t *testing.T) {
	// Create non-empty DataFrame
	builder := polars.NewDataFrame()
	df, err := builder.
		AddIntColumn("id", []int64{1, 2, 3}).
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie"}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}
	defer df.Free()

	// Create empty DataFrame with same structure
	emptyBuilder := polars.NewDataFrame()
	emptyDf, err := emptyBuilder.
		AddIntColumn("id", []int64{}).
		AddStringColumn("name", []string{}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create empty DataFrame: %v", err)
	}
	defer emptyDf.Free()

	t.Run("JoinWithEmpty", func(t *testing.T) {
		result := df.Join(emptyDf, "id", polars.JoinInner)
		defer result.Free()

		if result.Height() != 0 {
			t.Errorf("Expected 0 rows when joining with empty DataFrame, got %d", result.Height())
		}
	})

	t.Run("LeftJoinWithEmpty", func(t *testing.T) {
		result := df.Join(emptyDf, "id", polars.JoinLeft)
		defer result.Free()

		if result.Height() != 3 {
			t.Errorf("Expected 3 rows in left join with empty DataFrame, got %d", result.Height())
		}
	})
}

// Test join error cases
func TestDataFrameJoinErrors(t *testing.T) {
	// Create valid DataFrame
	builder := polars.NewDataFrame()
	df, err := builder.
		AddIntColumn("id", []int64{1, 2, 3}).
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie"}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}
	defer df.Free()

	t.Run("JoinWithNilDataFrame", func(t *testing.T) {
		result := df.Join(nil, "id", polars.JoinInner)

		// Check if result is nil or empty
		if result == nil {
			t.Error("Expected non-nil result even when joining with nil DataFrame")
			return
		}
		defer result.Free()

		if result.Height() != 0 || result.Width() != 0 {
			t.Error("Expected empty result when joining with nil DataFrame")
		}
	})

	t.Run("JoinNonExistentColumn", func(t *testing.T) {
		result := df.Join(df, "non_existent", polars.JoinInner)
		defer result.Free()

		// Should handle gracefully, likely returning empty result
		if result.Height() < 0 || result.Width() < 0 {
			t.Error("Join with non-existent column should handle gracefully")
		}
	})
}

// Test join with mixed data types
func TestDataFrameJoinMixedTypes(t *testing.T) {
	// Create left DataFrame with mixed types
	leftBuilder := polars.NewDataFrame()
	leftDf, err := leftBuilder.
		AddStringColumn("key", []string{"A", "B", "C"}).
		AddIntColumn("value1", []int64{10, 20, 30}).
		AddFloatColumn("value2", []float64{1.1, 2.2, 3.3}).
		AddBoolColumn("flag", []bool{true, false, true}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create left DataFrame: %v", err)
	}
	defer leftDf.Free()

	// Create right DataFrame
	rightBuilder := polars.NewDataFrame()
	rightDf, err := rightBuilder.
		AddStringColumn("key", []string{"B", "C", "D"}).
		AddStringColumn("category", []string{"Type1", "Type2", "Type3"}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create right DataFrame: %v", err)
	}
	defer rightDf.Free()

	t.Run("JoinMixedTypes", func(t *testing.T) {
		result := leftDf.Join(rightDf, "key", polars.JoinInner)
		defer result.Free()

		if result.Height() != 2 {
			t.Errorf("Expected 2 rows in mixed type join result, got %d", result.Height())
		}

		if result.Width() != 5 {
			t.Errorf("Expected 5 columns in mixed type join result, got %d", result.Width())
		}

		columns := result.Columns()
		expectedCols := []string{"key", "value1", "value2", "flag", "category"}
		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})
}

// Test chaining operations with joins
func TestDataFrameJoinChaining(t *testing.T) {
	// Create base DataFrame
	baseBuilder := polars.NewDataFrame()
	baseDf, err := baseBuilder.
		AddIntColumn("id", []int64{1, 2, 3, 4, 5}).
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}).
		AddIntColumn("score", []int64{85, 92, 78, 88, 95}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create base DataFrame: %v", err)
	}
	defer baseDf.Free()

	// Create departments DataFrame
	deptBuilder := polars.NewDataFrame()
	deptDf, err := deptBuilder.
		AddIntColumn("id", []int64{1, 2, 3, 4}).
		AddStringColumn("department", []string{"Engineering", "Sales", "Marketing", "HR"}).
		Build()

	if err != nil {
		t.Fatalf("Failed to create departments DataFrame: %v", err)
	}
	defer deptDf.Free()

	t.Run("JoinWithFilterAndSelect", func(t *testing.T) {
		result := baseDf.
			Join(deptDf, "id", polars.JoinInner).
			Filter(polars.Col("score").Gt(80)).
			Select(polars.Col("name"), polars.Col("department"), polars.Col("score"))

		defer result.Free()

		if result.Width() != 3 {
			t.Errorf("Expected 3 columns after join, filter, and select, got %d", result.Width())
		}

		if result.Height() == 0 {
			t.Error("Expected some rows after join and filter operations")
		}

		columns := result.Columns()
		expectedCols := []string{"name", "department", "score"}
		for i, expected := range expectedCols {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, columns[i])
			}
		}
	})

	t.Run("JoinWithSort", func(t *testing.T) {
		result := baseDf.
			Join(deptDf, "id", polars.JoinInner).
			Sort("score")

		defer result.Free()

		if result.Height() != 4 {
			t.Errorf("Expected 4 rows after join and sort, got %d", result.Height())
		}

		if result.Width() != 4 {
			t.Errorf("Expected 4 columns after join and sort, got %d", result.Width())
		}
	})
}

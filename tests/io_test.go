package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

// Test CSV I/O operations
func TestCSVOperations(t *testing.T) {
	df := loadTestData(t)

	t.Run("ReadCSV", func(t *testing.T) {
		// This is already tested in loadTestData, but let's be explicit
		csvPath := getTestDataPath()
		df, err := polars.ReadCSV(csvPath)
		
		if err != nil {
			t.Fatalf("Failed to read CSV: %v", err)
		}
		
		if df == nil {
			t.Fatal("DataFrame should not be nil")
		}
		
		if df.Height() == 0 {
			t.Error("DataFrame should have rows")
		}
		
		if df.Width() == 0 {
			t.Error("DataFrame should have columns")
		}
	})

	t.Run("WriteCSV", func(t *testing.T) {
		// Create a temporary file path
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "test_output.csv")
		
		// Write the dataframe to CSV
		err := df.WriteCSV(outputPath)
		if err != nil {
			t.Fatalf("Failed to write CSV: %v", err)
		}
		
		// Check if file exists
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			t.Error("Output CSV file was not created")
		}
		
		// Read the file back and verify
		readBackDf, err := polars.ReadCSV(outputPath)
		if err != nil {
			t.Fatalf("Failed to read back CSV: %v", err)
		}
		
		if readBackDf.Height() != df.Height() {
			t.Errorf("Read back DataFrame has different height: %d vs %d", readBackDf.Height(), df.Height())
		}
		
		if readBackDf.Width() != df.Width() {
			t.Errorf("Read back DataFrame has different width: %d vs %d", readBackDf.Width(), df.Width())
		}
		
		// Compare column names
		originalCols := df.Columns()
		readBackCols := readBackDf.Columns()
		
		for i, originalCol := range originalCols {
			if i >= len(readBackCols) || readBackCols[i] != originalCol {
				t.Errorf("Column %d mismatch: expected '%s', got '%s'", i, originalCol, readBackCols[i])
			}
		}
	})

	t.Run("WriteCSVFilteredData", func(t *testing.T) {
		// Filter data first
		filtered := df.Filter(polars.Col("petal.length").Gt(5))
		
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "filtered_output.csv")
		
		err := filtered.WriteCSV(outputPath)
		if err != nil {
			t.Fatalf("Failed to write filtered CSV: %v", err)
		}
		
		// Read back and verify
		readBack, err := polars.ReadCSV(outputPath)
		if err != nil {
			t.Fatalf("Failed to read back filtered CSV: %v", err)
		}
		
		if readBack.Height() != filtered.Height() {
			t.Errorf("Filtered CSV height mismatch: %d vs %d", readBack.Height(), filtered.Height())
		}
	})

	t.Run("ReadNonExistentCSV", func(t *testing.T) {
		_, err := polars.ReadCSV("non_existent_file.csv")
		if err == nil {
			t.Error("Expected error when reading non-existent CSV file")
		}
	})

	t.Run("WriteCSVInvalidPath", func(t *testing.T) {
		// Try to write to an invalid path (directory that doesn't exist)
		err := df.WriteCSV("/invalid/path/that/does/not/exist/file.csv")
		if err == nil {
			t.Error("Expected error when writing to invalid path")
		}
	})
}

// Test Parquet I/O operations
func TestParquetOperations(t *testing.T) {
	df := loadTestData(t)

	t.Run("WriteParquet", func(t *testing.T) {
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "test_output.parquet")
		
		err := df.WriteParquet(outputPath)
		if err != nil {
			t.Fatalf("Failed to write Parquet: %v", err)
		}
		
		// Check if file exists
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			t.Error("Output Parquet file was not created")
		}
		
		// Check file size (should be non-zero)
		fileInfo, err := os.Stat(outputPath)
		if err != nil {
			t.Fatalf("Failed to get file info: %v", err)
		}
		
		if fileInfo.Size() == 0 {
			t.Error("Parquet file should not be empty")
		}
	})

	t.Run("ReadParquet", func(t *testing.T) {
		// First write a parquet file
		tempDir := t.TempDir()
		parquetPath := filepath.Join(tempDir, "test.parquet")
		
		err := df.WriteParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to write Parquet for reading test: %v", err)
		}
		
		// Now read it back
		readBackDf, err := polars.ReadParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to read Parquet: %v", err)
		}
		
		if readBackDf == nil {
			t.Fatal("Read back DataFrame should not be nil")
		}
		
		if readBackDf.Height() != df.Height() {
			t.Errorf("Read back DataFrame has different height: %d vs %d", readBackDf.Height(), df.Height())
		}
		
		if readBackDf.Width() != df.Width() {
			t.Errorf("Read back DataFrame has different width: %d vs %d", readBackDf.Width(), df.Width())
		}
		
		// Compare column names
		originalCols := df.Columns()
		readBackCols := readBackDf.Columns()
		
		for i, originalCol := range originalCols {
			if i >= len(readBackCols) || readBackCols[i] != originalCol {
				t.Errorf("Column %d mismatch: expected '%s', got '%s'", i, originalCol, readBackCols[i])
			}
		}
	})

	t.Run("WriteReadParquetRoundTrip", func(t *testing.T) {
		// Test complete round trip with modified data
		modified := df.
			Filter(polars.Col("petal.length").Gt(2)).
			WithColumns(polars.Col("petal.length").MulValue(2.0).Alias("doubled_petal")).
			Select(polars.Col("variety"), polars.Col("petal.length"), polars.Col("doubled_petal"))
		
		tempDir := t.TempDir()
		parquetPath := filepath.Join(tempDir, "roundtrip.parquet")
		
		// Write
		err := modified.WriteParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to write modified DataFrame to Parquet: %v", err)
		}
		
		// Read back
		readBack, err := polars.ReadParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to read back Parquet: %v", err)
		}
		
		// Verify structure
		if readBack.Height() != modified.Height() {
			t.Errorf("Round trip height mismatch: %d vs %d", readBack.Height(), modified.Height())
		}
		
		if readBack.Width() != modified.Width() {
			t.Errorf("Round trip width mismatch: %d vs %d", readBack.Width(), modified.Width())
		}
		
		expectedCols := []string{"variety", "petal.length", "doubled_petal"}
		actualCols := readBack.Columns()
		
		for i, expected := range expectedCols {
			if i >= len(actualCols) || actualCols[i] != expected {
				t.Errorf("Round trip column %d mismatch: expected '%s', got '%s'", i, expected, actualCols[i])
			}
		}
	})

	t.Run("ReadNonExistentParquet", func(t *testing.T) {
		_, err := polars.ReadParquet("non_existent_file.parquet")
		if err == nil {
			t.Error("Expected error when reading non-existent Parquet file")
		}
	})

	t.Run("WriteParquetInvalidPath", func(t *testing.T) {
		err := df.WriteParquet("/invalid/path/that/does/not/exist/file.parquet")
		if err == nil {
			t.Error("Expected error when writing to invalid path")
		}
	})
}

// Test file format comparison
func TestFileFormatComparison(t *testing.T) {
	df := loadTestData(t)

	t.Run("CSVvsParquetSizeComparison", func(t *testing.T) {
		tempDir := t.TempDir()
		csvPath := filepath.Join(tempDir, "test.csv")
		parquetPath := filepath.Join(tempDir, "test.parquet")
		
		// Write both formats
		err := df.WriteCSV(csvPath)
		if err != nil {
			t.Fatalf("Failed to write CSV: %v", err)
		}
		
		err = df.WriteParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to write Parquet: %v", err)
		}
		
		// Get file sizes
		csvInfo, err := os.Stat(csvPath)
		if err != nil {
			t.Fatalf("Failed to get CSV file info: %v", err)
		}
		
		parquetInfo, err := os.Stat(parquetPath)
		if err != nil {
			t.Fatalf("Failed to get Parquet file info: %v", err)
		}
		
		// Both files should exist and have content
		if csvInfo.Size() == 0 {
			t.Error("CSV file should not be empty")
		}
		
		if parquetInfo.Size() == 0 {
			t.Error("Parquet file should not be empty")
		}
		
		// Log the sizes for information (not a requirement)
		t.Logf("CSV size: %d bytes, Parquet size: %d bytes", csvInfo.Size(), parquetInfo.Size())
	})

	t.Run("CSVvsParquetDataIntegrity", func(t *testing.T) {
		tempDir := t.TempDir()
		csvPath := filepath.Join(tempDir, "integrity_test.csv")
		parquetPath := filepath.Join(tempDir, "integrity_test.parquet")
		
		// Use a filtered dataset for this test
		filtered := df.Filter(polars.Col("petal.length").Gt(3))
		
		// Write both formats
		err := filtered.WriteCSV(csvPath)
		if err != nil {
			t.Fatalf("Failed to write CSV: %v", err)
		}
		
		err = filtered.WriteParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to write Parquet: %v", err)
		}
		
		// Read both back
		csvReadBack, err := polars.ReadCSV(csvPath)
		if err != nil {
			t.Fatalf("Failed to read CSV back: %v", err)
		}
		
		parquetReadBack, err := polars.ReadParquet(parquetPath)
		if err != nil {
			t.Fatalf("Failed to read Parquet back: %v", err)
		}
		
		// Compare dimensions
		if csvReadBack.Height() != parquetReadBack.Height() {
			t.Errorf("CSV and Parquet height mismatch: %d vs %d", csvReadBack.Height(), parquetReadBack.Height())
		}
		
		if csvReadBack.Width() != parquetReadBack.Width() {
			t.Errorf("CSV and Parquet width mismatch: %d vs %d", csvReadBack.Width(), parquetReadBack.Width())
		}
		
		// Compare columns
		csvCols := csvReadBack.Columns()
		parquetCols := parquetReadBack.Columns()
		
		for i, csvCol := range csvCols {
			if i >= len(parquetCols) || parquetCols[i] != csvCol {
				t.Errorf("Column %d mismatch between CSV and Parquet: '%s' vs '%s'", i, csvCol, parquetCols[i])
			}
		}
	})
}

// Benchmark I/O operations
func BenchmarkIOOperations(b *testing.B) {
	df := loadTestData(&testing.T{})
	tempDir := b.TempDir()

	b.Run("WriteCSV", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := filepath.Join(tempDir, "bench_output_"+string(rune(i))+".csv")
			err := df.WriteCSV(path)
			if err != nil {
				b.Fatalf("Failed to write CSV: %v", err)
			}
		}
	})

	b.Run("WriteParquet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := filepath.Join(tempDir, "bench_output_"+string(rune(i))+".parquet")
			err := df.WriteParquet(path)
			if err != nil {
				b.Fatalf("Failed to write Parquet: %v", err)
			}
		}
	})

	// Setup files for read benchmarks
	csvPath := filepath.Join(tempDir, "bench_read.csv")
	parquetPath := filepath.Join(tempDir, "bench_read.parquet")
	
	_ = df.WriteCSV(csvPath)
	_ = df.WriteParquet(parquetPath)

	b.Run("ReadCSV", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := polars.ReadCSV(csvPath)
			if err != nil {
				b.Fatalf("Failed to read CSV: %v", err)
			}
		}
	})

	b.Run("ReadParquet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := polars.ReadParquet(parquetPath)
			if err != nil {
				b.Fatalf("Failed to read Parquet: %v", err)
			}
		}
	})
}
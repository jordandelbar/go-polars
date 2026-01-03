package polars

import (
	"errors"
	"fmt"
	"unsafe"
)

/*
#include "polars_go.h"
#include <stdlib.h>
*/
import "C"

// ReadCSV reads a CSV file into a DataFrame.
func ReadCSV(filePath string) (*DataFrame, error) {
	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))

	df := C.read_csv(cPath)
	if df == nil || (*C.CDataFrame)(df).handle == nil {
		return nil, errors.New(C.GoString(C.get_last_error_message()))
	}

	return &DataFrame{ptr: (*C.CDataFrame)(df)}, nil
}

// ReadParquet reads a Parquet file into a DataFrame.
func ReadParquet(filePath string) (*DataFrame, error) {
	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))

	df := C.read_parquet(cPath)
	if df == nil || (*C.CDataFrame)(df).handle == nil {
		return nil, errors.New(C.GoString(C.get_last_error_message()))
	}

	return &DataFrame{ptr: (*C.CDataFrame)(df)}, nil
}

// ReadJSON reads a JSON file into a DataFrame.
// Supports the formats handled by Polars JsonReader (including JSON Lines / NDJSON).
func ReadJSON(filePath string) (*DataFrame, error) {
	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))

	df := C.read_json(cPath)
	if df == nil || (*C.CDataFrame)(df).handle == nil {
		return nil, errors.New(C.GoString(C.get_last_error_message()))
	}

	return &DataFrame{ptr: (*C.CDataFrame)(df)}, nil
}

// WriteCSV writes the DataFrame to a CSV file.
func (df DataFrame) WriteCSV(filePath string) error {
	if df.ptr == nil || df.ptr.handle == nil {
		return errors.New("write_csv error: dataframe is nil")
	}
	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))

	res := C.write_csv(df.ptr, cFilePath)

	if res == nil {
		return errors.New("write_csv error: unknown failure")
	}

	msg := C.GoString(res)
	if msg != "CSV written successfully" {
		return fmt.Errorf("write_csv error: %s", msg)
	}
	return nil
}

// WriteParquet writes the DataFrame to a Parquet file.
func (df DataFrame) WriteParquet(filePath string) error {
	if df.ptr == nil || df.ptr.handle == nil {
		return errors.New("write_parquet error: dataframe is nil")
	}
	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))

	res := C.write_parquet(df.ptr, cFilePath)

	if res == nil {
		return errors.New("write_parquet error: unknown failure")
	}

	msg := C.GoString(res)
	if msg != "Parquet written successfully" {
		return fmt.Errorf("write_parquet error: %s", msg)
	}
	return nil
}

// WriteJSON writes the DataFrame to a JSON file using Polars JsonWriter.
// Polars writes JSON Lines (one object per line).
func (df DataFrame) WriteJSON(filePath string) error {
	if df.ptr == nil || df.ptr.handle == nil {
		return errors.New("write_json error: dataframe is nil")
	}
	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))

	res := C.write_json(df.ptr, cFilePath)

	if res == nil {
		return errors.New("write_json error: unknown failure")
	}

	msg := C.GoString(res)
	if msg != "JSON written successfully" {
		return fmt.Errorf("write_json error: %s", msg)
	}
	return nil
}

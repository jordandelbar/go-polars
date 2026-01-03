package polars

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// ExcelOptions controls Excel reading behavior.
// InferTypes: try to detect bool/int64/float64; otherwise keep string.
// NullValues: values treated as null (case-sensitive match). TreatEmptyAsNull: empty string becomes null when true.
type ExcelOptions struct {
	SheetName        string
	InferTypes       bool
	NullValues       []string
	TreatEmptyAsNull bool
}

// ReadExcel keeps backward compatibility: no类型推断、空值视为""。
func ReadExcel(filePath, sheetName string) (*DataFrame, error) {
	return ReadExcelWithOptions(filePath, ExcelOptions{SheetName: sheetName})
}

// ReadExcelWithOptions loads an Excel sheet via excelize and builds a DataFrame entirely in memory.
// - 首行作表头，空表头自动命名 column_N。
// - 支持可选类型推断与空值处理。
// - 不落盘临时文件。
func ReadExcelWithOptions(filePath string, opts ExcelOptions) (*DataFrame, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("open excel: %w", err)
	}
	defer f.Close()

	sheet := opts.SheetName
	if strings.TrimSpace(sheet) == "" {
		sheet = f.GetSheetName(f.GetActiveSheetIndex())
		if sheet == "" {
			return nil, errors.New("no sheet found in workbook")
		}
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("read sheet %s: %w", sheet, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("sheet %s is empty", sheet)
	}

	headers := append([]string(nil), rows[0]...)
	if len(headers) == 0 {
		return nil, errors.New("header row is empty")
	}
	for i, h := range headers {
		if strings.TrimSpace(h) == "" {
			headers[i] = fmt.Sprintf("column_%d", i+1)
		}
	}

	dataRowCount := len(rows) - 1
	cols := make([][]string, len(headers))
	validity := make([][]bool, len(headers))
	for idx := range cols {
		cols[idx] = make([]string, 0, dataRowCount)
		validity[idx] = make([]bool, 0, dataRowCount)
	}

	isNull := func(val string) bool {
		if opts.TreatEmptyAsNull && strings.TrimSpace(val) == "" {
			return true
		}
		for _, nv := range opts.NullValues {
			if val == nv {
				return true
			}
		}
		return false
	}

	for _, row := range rows[1:] {
		if len(row) < len(headers) {
			padding := make([]string, len(headers)-len(row))
			row = append(row, padding...)
		}
		for i := range headers {
			val := row[i]
			null := isNull(val)
			cols[i] = append(cols[i], val)
			validity[i] = append(validity[i], !null)
		}
	}

	builder := NewDataFrame()
	for i, name := range headers {
		colVals := cols[i]
		colValidity := validity[i]

		if opts.InferTypes {
			switch detectExcelColumnKind(colVals, colValidity) {
			case excelKindBool:
				bools := make([]bool, len(colVals))
				for idx, v := range colVals {
					bval, _ := parseBool(v)
					bools[idx] = bval
				}
				builder.AddBoolColumnNullable(name, bools, colValidity)
			case excelKindInt:
				ints := make([]int64, len(colVals))
				for idx, v := range colVals {
					if !colValidity[idx] {
						continue
					}
					ival, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
					ints[idx] = ival
				}
				builder.AddIntColumnNullable(name, ints, colValidity)
			case excelKindFloat:
				floats := make([]float64, len(colVals))
				for idx, v := range colVals {
					if !colValidity[idx] {
						continue
					}
					fval, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
					floats[idx] = fval
				}
				builder.AddFloatColumnNullable(name, floats, colValidity)
			case excelKindDate:
				dates := make([]int32, len(colVals))
				for idx, v := range colVals {
					if !colValidity[idx] {
						continue
					}
					d, _ := parseDate(v)
					dates[idx] = d
				}
				builder.AddDateColumnNullable(name, dates, colValidity)
			case excelKindDatetimeMs:
				dts := make([]int64, len(colVals))
				for idx, v := range colVals {
					if !colValidity[idx] {
						continue
					}
					ts, _ := parseDatetimeMs(v)
					dts[idx] = ts
				}
				builder.AddDatetimeMsColumnNullable(name, dts, colValidity)
			default:
				builder.AddStringColumnNullable(name, colVals, colValidity)
			}
		} else {
			// Preserve旧行为：全字符串且空值仍视为 ""（除非用户明确要求 TreatEmptyAsNull/NullValues）。
			if opts.TreatEmptyAsNull || len(opts.NullValues) > 0 {
				builder.AddStringColumnNullable(name, colVals, colValidity)
			} else {
				builder.AddStringColumn(name, colVals)
			}
		}
	}

	return builder.Build()
}

type excelColumnKind int

const (
	excelKindString excelColumnKind = iota
	excelKindBool
	excelKindInt
	excelKindFloat
	excelKindDate
	excelKindDatetimeMs
)

func detectExcelColumnKind(values []string, validity []bool) excelColumnKind {
	hasValid := false
	allBool := true
	allInt := true
	allFloat := true
	allDate := true
	allDatetime := true

	for i, v := range values {
		if len(validity) != 0 && !validity[i] {
			continue
		}
		hasValid = true
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}

		if allBool {
			if _, ok := parseBool(trimmed); !ok {
				allBool = false
			}
		}
		if allInt {
			if _, err := strconv.ParseInt(trimmed, 10, 64); err != nil {
				allInt = false
			}
		}
		if allFloat {
			if _, err := strconv.ParseFloat(trimmed, 64); err != nil {
				allFloat = false
			}
		}
		if allDate {
			if _, ok := parseDate(trimmed); !ok {
				allDate = false
			}
		}
		if allDatetime {
			if _, ok := parseDatetimeMs(trimmed); !ok {
				allDatetime = false
			}
		}
	}

	if !hasValid {
		return excelKindString
	}
	if allBool {
		return excelKindBool
	}
	if allInt {
		return excelKindInt
	}
	if allFloat {
		return excelKindFloat
	}
	if allDate {
		return excelKindDate
	}
	if allDatetime {
		return excelKindDatetimeMs
	}
	return excelKindString
}

func parseBool(v string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "y", "on":
		return true, true
	case "false", "0", "no", "n", "off":
		return false, true
	default:
		return false, false
	}
}

var dateLayouts = []string{
	"2006-01-02",
	"2006/01/02",
	"20060102",
}

var datetimeLayouts = []string{
	"2006-01-02 15:04:05",
	"2006/01/02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.000",
	"2006-01-02T15:04:05.000",
}

// parseDate returns days since Unix epoch.
func parseDate(v string) (int32, bool) {
	trimmed := strings.TrimSpace(v)
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return int32(t.Unix() / 86400), true
		}
	}
	return 0, false
}

// parseDatetimeMs returns milliseconds since Unix epoch.
func parseDatetimeMs(v string) (int64, bool) {
	trimmed := strings.TrimSpace(v)
	for _, layout := range datetimeLayouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return t.UnixNano() / int64(time.Millisecond), true
		}
	}

	// Handle arbitrary fractional seconds length by generating layout dynamically.
	if idx := strings.Index(trimmed, "."); idx != -1 {
		fracLen := len(trimmed) - idx - 1
		if fracLen > 0 {
			if fracLen > 9 {
				fracLen = 9 // Go supports up to nanoseconds
			}
			layout := "2006-01-02 15:04:05." + strings.Repeat("0", fracLen)
			if t, err := time.Parse(layout, trimmed); err == nil {
				return t.UnixNano() / int64(time.Millisecond), true
			}
		}
	}
	return 0, false
}

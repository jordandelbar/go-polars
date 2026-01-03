package polars

/*
#include "polars_go.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"time"
	"unsafe"
)

// DataFrame represents a Polars DataFrame.
type DataFrame struct {
	ptr *C.CDataFrame
}

// Expr represents a Polars expression.
type Expr struct {
	ptr *C.CExpr
}

// Free releases the underlying C expression.
func (e *Expr) Free() {
	if e != nil && e.ptr != nil {
		C.free_expr(e.ptr)
		e.ptr = nil
	}
}

// GroupBy represents a Polars GroupBy operation.
type GroupBy struct {
	ptr *C.CGroupBy
}

// LazyFrame represents a Polars lazy plan.
type LazyFrame struct {
	ptr *C.CLazyFrame
}

// helper: convert map[string]DataType to C string slice and dtype slice.
func mapToCColumns(m map[string]DataType) ([]*C.char, []C.int32_t) {
	if len(m) == 0 {
		return nil, nil
	}

	cols := make([]*C.char, 0, len(m))
	dtypes := make([]C.int32_t, 0, len(m))
	for name, dt := range m {
		cName := C.CString(name)
		cols = append(cols, cName)
		dtypes = append(dtypes, C.int32_t(dt))
	}
	return cols, dtypes
}

// helper: free []*C.char
func freeCStringSlice(cols []*C.char) {
	for _, c := range cols {
		C.free(unsafe.Pointer(c))
	}
}

// ScanCSV creates a lazy plan that scans a CSV file with default scan options.
func ScanCSV(path string) *LazyFrame {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	lfPtr := C.scan_csv(cPath)
	if lfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &LazyFrame{}
	}

	return &LazyFrame{ptr: (*C.CLazyFrame)(lfPtr)}
}

// ScanParquet creates a lazy plan that scans a Parquet file with default scan options.
func ScanParquet(path string) *LazyFrame {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	lfPtr := C.scan_parquet(cPath)
	if lfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &LazyFrame{}
	}

	return &LazyFrame{ptr: (*C.CLazyFrame)(lfPtr)}
}

// ScanNDJSON creates a lazy plan that scans an NDJSON (JSON Lines) file with default scan options.
func ScanNDJSON(path string) *LazyFrame {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	lfPtr := C.scan_ndjson(cPath)
	if lfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &LazyFrame{}
	}

	return &LazyFrame{ptr: (*C.CLazyFrame)(lfPtr)}
}

// ScanCSVOptions configures CSV lazy scan (schema, overrides, inference, and error handling).
type ScanCSVOptions struct {
	Overrides           map[string]DataType // 部分列覆盖
	Schema              map[string]DataType // 全量 schema（可选）
	InferSchemaLength   int                 // <0 使用默认
	IgnoreErrors        bool                // true 则跳过解析错误的行
	TruncateRaggedLines bool                // true 则截断行中多余字段
	HasHeader           *bool               // nil 表示默认 true；false 关闭表头
}

// ScanCSVWithOptions creates a lazy CSV scan with schema/overrides controls.
func ScanCSVWithOptions(path string, opts ScanCSVOptions) *LazyFrame {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Convert map -> C arrays
	overCols, overDtypes := mapToCColumns(opts.Overrides)
	schemaCols, schemaDtypes := mapToCColumns(opts.Schema)

	defer freeCStringSlice(overCols)
	defer freeCStringSlice(schemaCols)

	overLen := C.int(len(overCols))
	schemaLen := C.int(len(schemaCols))
	infLen := C.int64_t(opts.InferSchemaLength)

	var overColsPtr **C.char
	var overDtypesPtr *C.int32_t
	if len(overCols) > 0 {
		overColsPtr = (**C.char)(unsafe.Pointer(&overCols[0]))
		overDtypesPtr = (*C.int32_t)(unsafe.Pointer(&overDtypes[0]))
	}

	var schemaColsPtr **C.char
	var schemaDtypesPtr *C.int32_t
	if len(schemaCols) > 0 {
		schemaColsPtr = (**C.char)(unsafe.Pointer(&schemaCols[0]))
		schemaDtypesPtr = (*C.int32_t)(unsafe.Pointer(&schemaDtypes[0]))
	}

	var cIgnore C.uint8_t
	if opts.IgnoreErrors {
		cIgnore = 1
	}
	var cTrunc C.uint8_t
	if opts.TruncateRaggedLines {
		cTrunc = 1
	}
	var cHasHeader C.uint8_t = 1
	if opts.HasHeader != nil {
		if !*opts.HasHeader {
			cHasHeader = 0
		}
	}

	lfPtr := C.scan_csv_with_schema(
		cPath,
		overColsPtr,
		overDtypesPtr,
		overLen,
		schemaColsPtr,
		schemaDtypesPtr,
		schemaLen,
		infLen,
		cIgnore,
		cTrunc,
		cHasHeader,
	)

	if lfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &LazyFrame{}
	}

	return &LazyFrame{ptr: (*C.CLazyFrame)(lfPtr)}
}

func (e Expr) Alias(name string) Expr {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	aliasPtr := C.expr_alias(e.ptr, cName) // Call the Rust function
	if aliasPtr == nil {
		log.Printf("error aliasing expression")
		return Expr{ptr: nil}
	}

	return Expr{ptr: (*C.CExpr)(aliasPtr)}
}

// String returns a string representation of the DataFrame.
func (df *DataFrame) String() string {
	if df.ptr == nil || df.ptr.handle == nil {
		return "<nil DataFrame>"
	}

	cStr := C.print_dataframe(df.ptr)
	if cStr == nil {
		return "<error printing DataFrame>"
	}
	defer C.free(unsafe.Pointer(cStr))

	return C.GoString(cStr)
}

// Free releases the memory associated with the DataFrame.
func (df *DataFrame) Free() {
	if df.ptr != nil {
		C.free_dataframe(df.ptr)
		df.ptr = nil
	}
}

// Width returns the number of columns in the DataFrame.
func (df *DataFrame) Width() int {
	if df == nil || df.ptr == nil || df.ptr.handle == nil {
		return 0
	}
	return int(C.dataframe_width(df.ptr))
}

// Height returns the number of rows in the DataFrame.
func (df *DataFrame) Height() int {
	if df == nil || df.ptr == nil || df.ptr.handle == nil {
		return 0
	}
	return int(C.dataframe_height(df.ptr))
}

// Drop removes one or more columns by name.
func (df *DataFrame) Drop(columns ...string) *DataFrame {
	if df == nil || df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &DataFrame{}
	}

	if len(columns) == 0 {
		log.Println("error: no columns provided to Drop")
		return &DataFrame{}
	}

	colsStr := ""
	for i, c := range columns {
		if i > 0 {
			colsStr += ","
		}
		colsStr += c
	}

	cCols := C.CString(colsStr)
	defer C.free(unsafe.Pointer(cCols))

	newDfPtr := C.drop_columns(df.ptr, cCols)
	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// IsEmpty reports whether the DataFrame has zero rows.
func (df *DataFrame) IsEmpty() bool {
	if df == nil || df.ptr == nil || df.ptr.handle == nil {
		return true
	}
	return df.Height() == 0
}

// Columns returns a list of column names in the DataFrame.
func (df *DataFrame) Columns() []string {
	if df == nil || df.ptr == nil || df.ptr.handle == nil {
		return nil
	}
	var names []string
	for i := 0; ; i++ {
		cStr := C.dataframe_column_name(df.ptr, C.size_t(i))
		if cStr == nil {
			break
		}
		defer C.free(unsafe.Pointer(cStr))
		names = append(names, C.GoString(cStr))
	}
	return names
}

// GroupBy creates a GroupBy operation on the specified columns.
func (df *DataFrame) GroupBy(columns ...string) *GroupBy {
	if df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &GroupBy{}
	}

	// Join column names with comma separator
	columnsStr := ""
	for i, col := range columns {
		if i > 0 {
			columnsStr += ","
		}
		columnsStr += col
	}

	cColumns := C.CString(columnsStr)
	defer C.free(unsafe.Pointer(cColumns))

	gbPtr := C.group_by(df.ptr, cColumns)
	if gbPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &GroupBy{}
	}

	return &GroupBy{ptr: (*C.CGroupBy)(gbPtr)}
}

// Lazy converts an eager DataFrame into a LazyFrame.
func (df *DataFrame) Lazy() *LazyFrame {
	if df == nil || df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &LazyFrame{}
	}

	ptr := C.dataframe_lazy(df.ptr)
	if ptr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &LazyFrame{}
	}

	return &LazyFrame{ptr: (*C.CLazyFrame)(ptr)}
}

// Collect executes the lazy plan. Optional streaming; defaults to false if not provided.
// Collect() -> streaming=false; Collect(true) -> streaming=true.
func (lf *LazyFrame) Collect(streaming ...bool) *DataFrame {
	if lf == nil || lf.ptr == nil {
		log.Println("error: LazyFrame is nil")
		return &DataFrame{}
	}

	useStreaming := false
	if len(streaming) > 0 {
		useStreaming = streaming[0]
	}

	var cStreaming C.uint8_t
	if useStreaming {
		cStreaming = 1
	}

	dfPtr := C.lazy_collect(lf.ptr, cStreaming)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// Free releases the LazyFrame.
func (lf *LazyFrame) Free() {
	if lf != nil && lf.ptr != nil {
		C.free_lazyframe(lf.ptr)
		lf.ptr = nil
	}
}

// Filter filters the DataFrame based on the given expression.
func (df *DataFrame) Filter(expr Expr) *DataFrame {
	filteredPtr := C.filter(df.ptr, expr.ptr)
	if filteredPtr == nil {
		err := errors.New(C.GoString(C.get_last_error_message()))
		log.Printf("Error while filtering: %s", err)
		return &DataFrame{}
	}
	return &DataFrame{ptr: (*C.CDataFrame)(filteredPtr)}
}

// Select allows selecting specific columns from the DataFrame.
func (df *DataFrame) Select(exprs ...Expr) *DataFrame {
	if df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &DataFrame{}
	}

	cExprs := make([]*C.CExpr, len(exprs))
	for i, expr := range exprs {
		cExprs[i] = expr.ptr
	}

	cExprsPtr := (**C.CExpr)(unsafe.Pointer(&cExprs[0]))
	cExprsLen := C.int(len(exprs))

	newDfPtr := C.select_columns(df.ptr, cExprsPtr, cExprsLen)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Col creates a new expression representing a column.
func Col(name string) Expr {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return Expr{ptr: (*C.CExpr)(C.col(cName))}
}

// Gt creates a "greater than" expression.
func (e Expr) Gt(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.int64_t(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.int64_t(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.int64_t(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_gt_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_gt_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		}
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.int64_t(intVal)))}
	default:
		panic("Gt: unsupported value type")
	}
}

// Lt creates a "less than" expression.
func (e Expr) Lt(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.int64_t(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.int64_t(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.int64_t(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_lt_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_lt_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		}
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.int64_t(intVal)))}
	default:
		panic("Lt: unsupported value type")
	}
}

// Eq creates an "equal to" expression.
func (e Expr) Eq(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.int64_t(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.int64_t(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.int64_t(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_eq_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_eq_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		}
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.int64_t(intVal)))}
	default:
		panic("Eq: unsupported value type")
	}
}

// EqString compares a column with a string literal (kept separate to avoid changing Eq panic semantics).
func (e Expr) EqString(value string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cStr := C.CString(value)
	defer C.free(unsafe.Pointer(cStr))
	return Expr{ptr: (*C.CExpr)(C.col_eq_str(e.ptr, cStr))}
}

// Ne creates a "not equal to" expression.
func (e Expr) Ne(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.int64_t(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.int64_t(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.int64_t(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_ne_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_ne_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		}
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.int64_t(intVal)))}
	default:
		panic("Ne: unsupported value type")
	}
}

// NeString compares a column with a string literal for inequality.
func (e Expr) NeString(value string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cStr := C.CString(value)
	defer C.free(unsafe.Pointer(cStr))
	return Expr{ptr: (*C.CExpr)(C.col_ne_str(e.ptr, cStr))}
}

// Ge creates a "greater than or equal to" expression.
func (e Expr) Ge(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.int64_t(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.int64_t(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.int64_t(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_ge_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_ge_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		}
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.int64_t(intVal)))}
	default:
		panic("Ge: unsupported value type")
	}
}

// Le creates a "less than or equal to" expression.
func (e Expr) Le(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.int64_t(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.int64_t(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.int64_t(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_le_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_le_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		}
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.int64_t(intVal)))}
	default:
		panic("Le: unsupported value type")
	}
}

// Add creates an addition expression between two expressions.
func (e Expr) Add(other Expr) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_add(e.ptr, other.ptr))}
}

// Sub creates a subtraction expression between two expressions.
func (e Expr) Sub(other Expr) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_sub(e.ptr, other.ptr))}
}

// Mul creates a multiplication expression between two expressions.
func (e Expr) Mul(other Expr) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_mul(e.ptr, other.ptr))}
}

// Div creates a division expression between two expressions.
func (e Expr) Div(other Expr) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_div(e.ptr, other.ptr))}
}

// AddValue creates an addition expression with a numeric value.
func (e Expr) AddValue(value float64) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_add_value(e.ptr, C.double(value)))}
}

// SubValue creates a subtraction expression with a numeric value.
func (e Expr) SubValue(value float64) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_sub_value(e.ptr, C.double(value)))}
}

// MulValue creates a multiplication expression with a numeric value.
func (e Expr) MulValue(value float64) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_mul_value(e.ptr, C.double(value)))}
}

// DivValue creates a division expression with a numeric value.
func (e Expr) DivValue(value float64) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_div_value(e.ptr, C.double(value)))}
}

// And creates a logical AND expression between two expressions.
func (e Expr) And(other Expr) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_and(e.ptr, other.ptr))}
}

// Or creates a logical OR expression between two expressions.
func (e Expr) Or(other Expr) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_or(e.ptr, other.ptr))}
}

// Not creates a logical NOT expression.
func (e Expr) Not() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_not(e.ptr))}
}

// IsNull checks for null values.
func (e Expr) IsNull() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_is_null(e.ptr))}
}

// IsNotNull checks for non-null values.
func (e Expr) IsNotNull() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_is_not_null(e.ptr))}
}

// FillNull replaces nulls with a literal value (int64, float64, string, or bool).
func (e Expr) FillNull(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.expr_fill_null_int64(e.ptr, C.longlong(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.expr_fill_null_int64(e.ptr, C.longlong(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.expr_fill_null_f64(e.ptr, C.double(v)))}
	case string:
		cVal := C.CString(v)
		defer C.free(unsafe.Pointer(cVal))
		return Expr{ptr: (*C.CExpr)(C.expr_fill_null_str(e.ptr, cVal))}
	case bool:
		var b C.uchar
		if v {
			b = 1
		}
		return Expr{ptr: (*C.CExpr)(C.expr_fill_null_bool(e.ptr, b))}
	default:
		panic("FillNull: unsupported type; use int64, float64, string, or bool")
	}
}

// String namespace helpers (mirror Polars str namespace)
// These functions accept a Go string pattern; when a `literal` flag is provided,
// set it to false to treat the pattern as a regex (powered by Polars), or true for plain substring matching.
func (e Expr) StrStripChars(chars string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	var cChars *C.char
	if chars != "" {
		cChars = C.CString(chars)
		defer C.free(unsafe.Pointer(cChars))
	}
	ptr := C.expr_str_strip_chars(e.ptr, cChars)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

func (e Expr) StrToLower() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_str_to_lowercase(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

func (e Expr) StrToUpper() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_str_to_uppercase(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// StrContains checks whether the string contains the given pattern.
// literal=false enables regex search; literal=true does a plain substring search.
func (e Expr) StrContains(pattern string, literal bool) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cPat := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPat))
	var litFlag C.uint8_t
	if literal {
		litFlag = 1
	}
	ptr := C.expr_str_contains(e.ptr, cPat, litFlag)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

func (e Expr) StrStartsWith(prefix string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cPre := C.CString(prefix)
	defer C.free(unsafe.Pointer(cPre))
	ptr := C.expr_str_starts_with(e.ptr, cPre)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

func (e Expr) StrEndsWith(suffix string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cSuf := C.CString(suffix)
	defer C.free(unsafe.Pointer(cSuf))
	ptr := C.expr_str_ends_with(e.ptr, cSuf)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

func (e Expr) StrLenChars() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_str_len_chars(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// StrReplace replaces the first match of pattern with value.
// literal=false uses regex replace; literal=true performs literal matching.
func (e Expr) StrReplace(pattern, value string, literal bool) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cPat := C.CString(pattern)
	cVal := C.CString(value)
	defer C.free(unsafe.Pointer(cPat))
	defer C.free(unsafe.Pointer(cVal))
	var litFlag C.uint8_t
	if literal {
		litFlag = 1
	}
	ptr := C.expr_str_replace(e.ptr, cPat, cVal, litFlag)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

func (e Expr) StrReplaceAll(pattern, value string, literal bool) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cPat := C.CString(pattern)
	cVal := C.CString(value)
	defer C.free(unsafe.Pointer(cPat))
	defer C.free(unsafe.Pointer(cVal))
	var litFlag C.uint8_t
	if literal {
		litFlag = 1
	}
	ptr := C.expr_str_replace_all(e.ptr, cPat, cVal, litFlag)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// StrSlice extracts a substring; length=-1 means no upper bound.
func (e Expr) StrSlice(offset, length int64) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_str_slice(e.ptr, C.int64_t(offset), C.int64_t(length))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// StrExtract extracts a regex capture group; groupIndex is zero-based.
func (e Expr) StrExtract(pattern string, groupIndex uint32) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cPat := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPat))
	ptr := C.expr_str_extract(e.ptr, cPat, C.uint32_t(groupIndex))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// StrSplit splits by a delimiter into a list column.
func (e Expr) StrSplit(by string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cBy := C.CString(by)
	defer C.free(unsafe.Pointer(cBy))
	ptr := C.expr_str_split(e.ptr, cBy)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// StrSplitInclusive splits and keeps the delimiter.
func (e Expr) StrSplitInclusive(by string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cBy := C.CString(by)
	defer C.free(unsafe.Pointer(cBy))
	ptr := C.expr_str_split_inclusive(e.ptr, cBy)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// List namespace helpers
// These operate on list-typed columns row-wise.
// ListLen returns the number of elements in each list.
func (e Expr) ListLen() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_len(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListFirst returns the first element of each list.
func (e Expr) ListFirst() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_first(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListLast returns the last element of each list.
func (e Expr) ListLast() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_last(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListGet fetches the element at the given index for each list.
func (e Expr) ListGet(index int64) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_get(e.ptr, C.int64_t(index))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListJoin concatenates list elements into a string using the separator.
func (e Expr) ListJoin(separator string) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	cSep := C.CString(separator)
	defer C.free(unsafe.Pointer(cSep))
	ptr := C.expr_list_join(e.ptr, cSep)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListSum sums numeric elements inside each list.
func (e Expr) ListSum() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_sum(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListMax returns the max element per list.
func (e Expr) ListMax() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_max(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListMin returns the min element per list.
func (e Expr) ListMin() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_min(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListMean computes the mean of numeric elements per list.
func (e Expr) ListMean() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_mean(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListSort sorts each list; descending controls order, nullsLast pushes nulls to the end.
func (e Expr) ListSort(descending bool, nullsLast bool) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	var cDesc C.uint8_t
	if descending {
		cDesc = 1
	}
	var cNullsLast C.uint8_t
	if nullsLast {
		cNullsLast = 1
	}
	ptr := C.expr_list_sort(e.ptr, cDesc, cNullsLast)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListReverse reverses each list.
func (e Expr) ListReverse() Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_reverse(e.ptr)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListUnique drops duplicates per list; maintainOrder keeps the original order when true.
func (e Expr) ListUnique(maintainOrder bool) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	var cMaintain C.uint8_t
	if maintainOrder {
		cMaintain = 1
	}
	ptr := C.expr_list_unique(e.ptr, cMaintain)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListHead takes the first n elements of each list.
func (e Expr) ListHead(length int64) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_head(e.ptr, C.int64_t(length))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListTail takes the last n elements of each list.
func (e Expr) ListTail(length int64) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_tail(e.ptr, C.int64_t(length))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ListSlice slices each list by offset and length.
func (e Expr) ListSlice(offset int64, length int64) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	ptr := C.expr_list_slice(e.ptr, C.int64_t(offset), C.int64_t(length))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ========== Str Namespace ==========

// ExprStr provides string operations on expressions (mirrors polars.Expr.str).
type ExprStr struct {
	expr Expr
}

// Str returns the string namespace for this expression.
func (e Expr) Str() ExprStr {
	return ExprStr{expr: e}
}

// StripChars removes leading and trailing characters from strings.
func (s ExprStr) StripChars(chars string) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	var cChars *C.char
	if chars == "" {
		cChars = nil
	} else {
		cChars = C.CString(chars)
		defer C.free(unsafe.Pointer(cChars))
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_strip_chars(s.expr.ptr, cChars))}
}

// ToLowercase converts strings to lowercase.
func (s ExprStr) ToLowercase() Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_to_lowercase(s.expr.ptr))}
}

// ToUppercase converts strings to uppercase.
func (s ExprStr) ToUppercase() Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_to_uppercase(s.expr.ptr))}
}

// Contains checks if strings contain a pattern. Set literal=true for literal matching.
func (s ExprStr) Contains(pattern string, literal bool) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))
	var cLiteral C.uint8_t
	if literal {
		cLiteral = 1
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_contains(s.expr.ptr, cPattern, cLiteral))}
}

// StartsWith checks if strings start with a prefix.
func (s ExprStr) StartsWith(prefix string) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cPrefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cPrefix))
	return Expr{ptr: (*C.CExpr)(C.expr_str_starts_with(s.expr.ptr, cPrefix))}
}

// EndsWith checks if strings end with a suffix.
func (s ExprStr) EndsWith(suffix string) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cSuffix := C.CString(suffix)
	defer C.free(unsafe.Pointer(cSuffix))
	return Expr{ptr: (*C.CExpr)(C.expr_str_ends_with(s.expr.ptr, cSuffix))}
}

// LenChars returns the length of strings in characters.
func (s ExprStr) LenChars() Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_len_chars(s.expr.ptr))}
}

// Replace replaces the first occurrence of pattern with value. Set literal=true for literal matching.
func (s ExprStr) Replace(pattern, value string, literal bool) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	var cLiteral C.uint8_t
	if literal {
		cLiteral = 1
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_replace(s.expr.ptr, cPattern, cValue, cLiteral))}
}

// ReplaceAll replaces all occurrences of pattern with value. Set literal=true for literal matching.
func (s ExprStr) ReplaceAll(pattern, value string, literal bool) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	var cLiteral C.uint8_t
	if literal {
		cLiteral = 1
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_replace_all(s.expr.ptr, cPattern, cValue, cLiteral))}
}

// Slice extracts a substring. Use length=-1 for no limit.
func (s ExprStr) Slice(offset int64, length int64) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_str_slice(s.expr.ptr, C.int64_t(offset), C.int64_t(length)))}
}

// Extract captures a regex group; groupIndex is zero-based.
func (s ExprStr) Extract(pattern string, groupIndex uint32) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cPat := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPat))
	ptr := C.expr_str_extract(s.expr.ptr, cPat, C.uint32_t(groupIndex))
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// Split splits strings by a delimiter into a list column.
func (s ExprStr) Split(by string) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cBy := C.CString(by)
	defer C.free(unsafe.Pointer(cBy))
	ptr := C.expr_str_split(s.expr.ptr, cBy)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// SplitInclusive splits strings and keeps the delimiter.
func (s ExprStr) SplitInclusive(by string) Expr {
	if s.expr.ptr == nil {
		return Expr{}
	}
	cBy := C.CString(by)
	defer C.free(unsafe.Pointer(cBy))
	ptr := C.expr_str_split_inclusive(s.expr.ptr, cBy)
	if ptr == nil {
		log.Printf("error: %s", C.GoString(C.get_last_error_message()))
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(ptr)}
}

// ========== List Namespace ==========

// ExprList provides list operations on expressions (mirrors polars.Expr.list).
type ExprList struct {
	expr Expr
}

// List returns the list namespace for this expression.
func (e Expr) List() ExprList {
	return ExprList{expr: e}
}

// Len returns the length of lists.
func (l ExprList) Len() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_len(l.expr.ptr))}
}

// First returns the first element of lists.
func (l ExprList) First() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_first(l.expr.ptr))}
}

// Last returns the last element of lists.
func (l ExprList) Last() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_last(l.expr.ptr))}
}

// Get returns the element at the given index.
func (l ExprList) Get(index int64) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_get(l.expr.ptr, C.int64_t(index)))}
}

// Join concatenates list elements into a string with the given separator.
func (l ExprList) Join(separator string) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	cSep := C.CString(separator)
	defer C.free(unsafe.Pointer(cSep))
	return Expr{ptr: (*C.CExpr)(C.expr_list_join(l.expr.ptr, cSep))}
}

// Sum reduces list elements by summing them.
func (l ExprList) Sum() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_sum(l.expr.ptr))}
}

// Max returns the maximum element in the list.
func (l ExprList) Max() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_max(l.expr.ptr))}
}

// Min returns the minimum element in the list.
func (l ExprList) Min() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_min(l.expr.ptr))}
}

// Mean returns the mean of list elements.
func (l ExprList) Mean() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_mean(l.expr.ptr))}
}

// Sort orders list elements.
func (l ExprList) Sort(descending bool, nullsLast bool) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	var cDesc C.uint8_t
	if descending {
		cDesc = 1
	}
	var cNulls C.uint8_t
	if nullsLast {
		cNulls = 1
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_sort(l.expr.ptr, cDesc, cNulls))}
}

// Reverse reverses list elements.
func (l ExprList) Reverse() Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_reverse(l.expr.ptr))}
}

// Unique removes duplicate elements optionally keeping order.
func (l ExprList) Unique(maintainOrder bool) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	var cMaintain C.uint8_t
	if maintainOrder {
		cMaintain = 1
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_unique(l.expr.ptr, cMaintain))}
}

// Head returns the first n elements.
func (l ExprList) Head(length int64) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_head(l.expr.ptr, C.int64_t(length)))}
}

// Tail returns the last n elements.
func (l ExprList) Tail(length int64) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_tail(l.expr.ptr, C.int64_t(length)))}
}

// Slice trims lists with offset and length.
func (l ExprList) Slice(offset int64, length int64) Expr {
	if l.expr.ptr == nil {
		return Expr{}
	}
	return Expr{ptr: (*C.CExpr)(C.expr_list_slice(l.expr.ptr, C.int64_t(offset), C.int64_t(length)))}
}

// ========== is_in ==========

// IsIn checks if values are in the given list. Supports []string, []int64, []float64.
func (e Expr) IsIn(values interface{}) Expr {
	if e.ptr == nil {
		return Expr{}
	}
	switch v := values.(type) {
	case []string:
		if len(v) == 0 {
			log.Println("error: IsIn requires at least one value")
			return Expr{}
		}
		cStrings := make([]*C.char, len(v))
		for i, s := range v {
			cStrings[i] = C.CString(s)
		}
		defer func() {
			for _, cs := range cStrings {
				C.free(unsafe.Pointer(cs))
			}
		}()
		return Expr{ptr: (*C.CExpr)(C.expr_is_in_str(e.ptr, (**C.char)(unsafe.Pointer(&cStrings[0])), C.int(len(v))))}
	case []int64:
		if len(v) == 0 {
			log.Println("error: IsIn requires at least one value")
			return Expr{}
		}
		return Expr{ptr: (*C.CExpr)(C.expr_is_in_int64(e.ptr, (*C.int64_t)(unsafe.Pointer(&v[0])), C.int(len(v))))}
	case []int:
		if len(v) == 0 {
			log.Println("error: IsIn requires at least one value")
			return Expr{}
		}
		int64s := make([]int64, len(v))
		for i, val := range v {
			int64s[i] = int64(val)
		}
		return Expr{ptr: (*C.CExpr)(C.expr_is_in_int64(e.ptr, (*C.int64_t)(unsafe.Pointer(&int64s[0])), C.int(len(int64s))))}
	case []float64:
		if len(v) == 0 {
			log.Println("error: IsIn requires at least one value")
			return Expr{}
		}
		return Expr{ptr: (*C.CExpr)(C.expr_is_in_f64(e.ptr, (*C.double)(unsafe.Pointer(&v[0])), C.int(len(v))))}
	default:
		panic(fmt.Sprintf("IsIn: unsupported value type %T", values))
	}
}

// Head returns the first n rows of the DataFrame.
func (df DataFrame) Head(n int) *DataFrame {
	cHeadDf := C.head(df.ptr, C.size_t(n))

	if cHeadDf == nil || (*C.CDataFrame)(cHeadDf).handle == nil {
		err := C.GoString(C.get_last_error_message())
		log.Printf("Error getting head: %s", err)
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(cHeadDf)}
}

// WithColumns adds or replaces columns in the DataFrame.
func (df *DataFrame) WithColumns(exprs ...Expr) *DataFrame {
	cExprs := make([]*C.CExpr, len(exprs))
	for i, expr := range exprs {
		cExprs[i] = expr.ptr
	}

	cExprsPtr := (**C.CExpr)(unsafe.Pointer(&cExprs[0]))
	cExprsLen := C.int(len(exprs))

	newDfPtr := C.with_columns(df.ptr, cExprsPtr, cExprsLen)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Lit creates a literal expression.
func Lit(value interface{}) Expr {
	var cExpr *C.CExpr

	switch v := value.(type) {
	case int64:
		cExpr = C.lit_int64(C.int64_t(v))
	case int32:
		cExpr = C.lit_int32(C.int(v))
	case int:
		cExpr = C.lit_int64(C.int64_t(v)) // Treat as int64
	case float64:
		cExpr = C.lit_float64(C.double(v))
	case float32:
		cExpr = C.lit_float32(C.float(v))
	case string:
		cStr := C.CString(v)
		defer C.free(unsafe.Pointer(cStr))
		cExpr = C.lit_string(cStr)
	case bool:
		cExpr = C.lit_bool(C.uint8_t(0))
		if v {
			cExpr = C.lit_bool(C.uint8_t(1))
		}
	default:
		panic(fmt.Sprintf("Unsupported literal type: %T", value))
	}

	return Expr{ptr: (*C.CExpr)(cExpr)}
}

type whenBranch struct {
	condition Expr
	result    Expr
}

type whenBuilder struct {
	branches []whenBranch
}

type whenConditionStage struct {
	builder   *whenBuilder
	condition Expr
}

// When starts a Polars when/then/otherwise expression chain.
func When(condition Expr) *whenConditionStage {
	return (&whenBuilder{}).When(condition)
}

func (wb *whenBuilder) When(condition Expr) *whenConditionStage {
	if wb == nil {
		wb = &whenBuilder{}
	}
	if condition.ptr == nil {
		log.Println("error: When condition expression is nil")
	}
	return &whenConditionStage{builder: wb, condition: condition}
}

func (stage *whenConditionStage) Then(result Expr) *whenBuilder {
	if stage == nil || stage.builder == nil {
		log.Println("error: When chain is not initialized before Then")
		return &whenBuilder{}
	}
	if stage.condition.ptr == nil || result.ptr == nil {
		log.Println("error: When.Then requires non-nil expressions")
		return stage.builder
	}
	stage.builder.branches = append(stage.builder.branches, whenBranch{condition: stage.condition, result: result})
	return stage.builder
}

func (stage *whenConditionStage) ThenValue(value interface{}) *whenBuilder {
	return stage.Then(Lit(value))
}

func (wb *whenBuilder) Otherwise(expr Expr) Expr {
	if wb == nil || len(wb.branches) == 0 {
		log.Println("error: Otherwise requires at least one preceding When/Then branch")
		return Expr{}
	}
	if expr.ptr == nil {
		log.Println("error: Otherwise expression is nil")
		return Expr{}
	}
	return buildWhenExpr(wb.branches, expr)
}

func (wb *whenBuilder) OtherwiseValue(value interface{}) Expr {
	return wb.Otherwise(Lit(value))
}

func buildWhenExpr(branches []whenBranch, otherwise Expr) Expr {
	if len(branches) == 0 {
		log.Println("error: When expression requires at least one branch")
		return Expr{}
	}
	if otherwise.ptr == nil {
		log.Println("error: Otherwise expression is nil")
		return Expr{}
	}
	condPtrs := make([]*C.CExpr, len(branches))
	thenPtrs := make([]*C.CExpr, len(branches))
	for i, branch := range branches {
		if branch.condition.ptr == nil || branch.result.ptr == nil {
			log.Println("error: When branch contains nil expressions")
			return Expr{}
		}
		condPtrs[i] = branch.condition.ptr
		thenPtrs[i] = branch.result.ptr
	}

	exprPtr := C.expr_when_then_otherwise(
		(**C.CExpr)(unsafe.Pointer(&condPtrs[0])),
		(**C.CExpr)(unsafe.Pointer(&thenPtrs[0])),
		C.int(len(branches)),
		otherwise.ptr,
	)
	if exprPtr == nil {
		if err := C.GoString(C.get_last_error_message()); err != "" {
			log.Printf("error building when expression: %s", err)
		}
		return Expr{}
	}

	return Expr{ptr: (*C.CExpr)(exprPtr)}
}

// Free releases the memory associated with the GroupBy.
func (gb *GroupBy) Free() {
	if gb.ptr != nil {
		C.free_groupby(gb.ptr)
		gb.ptr = nil
	}
}

// Agg performs aggregation operations on the GroupBy.
func (gb *GroupBy) Agg(exprs ...Expr) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cExprs := make([]*C.CExpr, len(exprs))
	for i, expr := range exprs {
		cExprs[i] = expr.ptr
	}

	cExprsPtr := (**C.CExpr)(unsafe.Pointer(&cExprs[0]))
	cExprsLen := C.int(len(exprs))

	newDfPtr := C.groupby_agg(gb.ptr, cExprsPtr, cExprsLen)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Sum calculates the sum of the specified column for each group.
func (gb *GroupBy) Sum(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	newDfPtr := C.groupby_sum(gb.ptr, cColumn)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Mean calculates the mean of the specified column for each group.
func (gb *GroupBy) Mean(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	newDfPtr := C.groupby_mean(gb.ptr, cColumn)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Count calculates the count of rows for each group.
func (gb *GroupBy) Count() *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	newDfPtr := C.groupby_count(gb.ptr)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Min calculates the minimum of the specified column for each group.
func (gb *GroupBy) Min(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	newDfPtr := C.groupby_min(gb.ptr, cColumn)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Max calculates the maximum of the specified column for each group.
func (gb *GroupBy) Max(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	newDfPtr := C.groupby_max(gb.ptr, cColumn)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Std calculates the standard deviation of the specified column for each group.
func (gb *GroupBy) Std(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	newDfPtr := C.groupby_std(gb.ptr, cColumn)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

// Var calculates the variance of the specified column for each group.
func (gb *GroupBy) Var(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_var(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// Quantile calculates the given quantile for the specified column for each group.
func (gb *GroupBy) Quantile(column string, quantile float64) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_quantile(gb.ptr, cColumn, C.double(quantile))
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// Median calculates the median of the specified column for each group.
func (gb *GroupBy) Median(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_median(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// Product calculates the product of the specified column for each group.
func (gb *GroupBy) Product(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_product(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// First selects the first value of the specified column for each group.
func (gb *GroupBy) First(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_first(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// Last selects the last value of the specified column for each group.
func (gb *GroupBy) Last(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_last(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// NUnique counts unique values of the specified column for each group.
func (gb *GroupBy) NUnique(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_n_unique(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// ApproxNUnique approximates unique counts of the specified column for each group.
func (gb *GroupBy) ApproxNUnique(column string) *DataFrame {
	if gb.ptr == nil {
		log.Println("error: GroupBy is nil")
		return &DataFrame{}
	}

	cColumn := C.CString(column)
	defer C.free(unsafe.Pointer(cColumn))

	dfPtr := C.groupby_approx_n_unique(gb.ptr, cColumn)
	if dfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}
}

// Sum creates a sum aggregation expression.
func (e Expr) Sum() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_sum(e.ptr))}
}

// Mean creates a mean aggregation expression.
func (e Expr) Mean() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_mean(e.ptr))}
}

// Min creates a min aggregation expression.
func (e Expr) Min() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_min(e.ptr))}
}

// Max creates a max aggregation expression.
func (e Expr) Max() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_max(e.ptr))}
}

// Std creates a standard deviation aggregation expression.
func (e Expr) Std() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_std(e.ptr))}
}

// Var creates a variance aggregation expression.
func (e Expr) Var() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_var(e.ptr))}
}

// Quantile creates a quantile aggregation expression.
func (e Expr) Quantile(quantile float64) Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_quantile(e.ptr, C.double(quantile)))}
}

// Median creates a median aggregation expression.
func (e Expr) Median() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_median(e.ptr))}
}

// Product creates a product aggregation expression.
func (e Expr) Product() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_product(e.ptr))}
}

// First selects the first value in an aggregation context.
func (e Expr) First() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_first(e.ptr))}
}

// Last selects the last value in an aggregation context.
func (e Expr) Last() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_last(e.ptr))}
}

// NUnique creates a unique count aggregation expression.
func (e Expr) NUnique() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_n_unique(e.ptr))}
}

// ApproxNUnique creates an approximate unique count aggregation expression.
func (e Expr) ApproxNUnique() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_approx_n_unique(e.ptr))}
}

// Count creates a count aggregation expression.
func Count() Expr {
	return Expr{ptr: (*C.CExpr)(C.expr_count())}
}

// Sort sorts the DataFrame by one or more columns in ascending order.
func (df *DataFrame) Sort(columns ...string) *DataFrame {
	if df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &DataFrame{}
	}

	// Join column names with comma separator
	columnsStr := ""
	for i, col := range columns {
		if i > 0 {
			columnsStr += ","
		}
		columnsStr += col
	}

	cColumns := C.CString(columnsStr)
	defer C.free(unsafe.Pointer(cColumns))

	// All ascending (false for descending)
	descendingStr := ""
	for i := range columns {
		if i > 0 {
			descendingStr += ","
		}
		descendingStr += "false"
	}

	cDescending := C.CString(descendingStr)
	defer C.free(unsafe.Pointer(cDescending))

	sortedPtr := C.sort_by_columns(df.ptr, cColumns, cDescending)
	if sortedPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(sortedPtr)}
}

// JoinType represents the type of join operation
type JoinType string

const (
	JoinInner JoinType = "inner"
	JoinLeft  JoinType = "left"
	JoinRight JoinType = "right"
	JoinOuter JoinType = "outer"
)

// Join performs a join operation with another DataFrame on matching columns
func (df *DataFrame) Join(other *DataFrame, on string, how JoinType) *DataFrame {
	return df.JoinOn(other, on, on, how)
}

// JoinOn performs a join operation with another DataFrame using different column names for left and right
func (df *DataFrame) JoinOn(other *DataFrame, leftOn, rightOn string, how JoinType) *DataFrame {
	if df == nil || df.ptr == nil {
		log.Println("error: left DataFrame is nil")
		return &DataFrame{}
	}

	if other == nil || other.ptr == nil {
		log.Println("error: right DataFrame is nil")
		return &DataFrame{}
	}

	cLeftOn := C.CString(leftOn)
	defer C.free(unsafe.Pointer(cLeftOn))

	cRightOn := C.CString(rightOn)
	defer C.free(unsafe.Pointer(cRightOn))

	var cJoinType C.CJoinType
	switch how {
	case JoinInner:
		cJoinType = C.JOIN_INNER
	case JoinLeft:
		cJoinType = C.JOIN_LEFT
	case JoinRight:
		cJoinType = C.JOIN_RIGHT
	case JoinOuter:
		cJoinType = C.JOIN_OUTER
	default:
		log.Printf("error: unknown join type %s", how)
		return &DataFrame{}
	}

	joinedPtr := C.join_dataframes(df.ptr, other.ptr, cLeftOn, cRightOn, cJoinType)
	if joinedPtr == nil {
		err := errors.New(C.GoString(C.get_last_error_message()))
		log.Printf("Error while joining: %s", err)
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(joinedPtr)}
}

// JoinMultiple performs a join operation with multiple key columns
// leftOn and rightOn should be comma-separated column names
func (df *DataFrame) JoinMultiple(other *DataFrame, leftOn, rightOn string, how JoinType) *DataFrame {
	if df == nil || df.ptr == nil {
		log.Println("error: left DataFrame is nil")
		return &DataFrame{}
	}

	if other == nil || other.ptr == nil {
		log.Println("error: right DataFrame is nil")
		return &DataFrame{}
	}

	cLeftOn := C.CString(leftOn)
	defer C.free(unsafe.Pointer(cLeftOn))

	cRightOn := C.CString(rightOn)
	defer C.free(unsafe.Pointer(cRightOn))

	var cJoinType C.CJoinType
	switch how {
	case JoinInner:
		cJoinType = C.JOIN_INNER
	case JoinLeft:
		cJoinType = C.JOIN_LEFT
	case JoinRight:
		cJoinType = C.JOIN_RIGHT
	case JoinOuter:
		cJoinType = C.JOIN_OUTER
	default:
		log.Printf("error: unknown join type %s", how)
		return &DataFrame{}
	}

	joinedPtr := C.join_dataframes_multiple_keys(df.ptr, other.ptr, cLeftOn, cRightOn, cJoinType)
	if joinedPtr == nil {
		err := errors.New(C.GoString(C.get_last_error_message()))
		log.Printf("Error while joining: %s", err)
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(joinedPtr)}
}

// ConcatRows vertically stacks multiple DataFrames. Schemas must align.
// If rechunk is true, the result is consolidated into a single chunk.
func ConcatRows(rechunk bool, dfs ...*DataFrame) *DataFrame {
	if len(dfs) == 0 {
		log.Println("error: no dataframes provided to ConcatRows")
		return &DataFrame{}
	}

	cDfs := make([]*C.CDataFrame, len(dfs))
	for i, d := range dfs {
		if d == nil || d.ptr == nil {
			log.Println("error: ConcatRows encountered nil dataframe")
			return &DataFrame{}
		}
		cDfs[i] = d.ptr
	}

	var cRechunk C.uint8_t
	if rechunk {
		cRechunk = 1
	}

	ptr := C.concat_df_vertical((**C.CDataFrame)(unsafe.Pointer(&cDfs[0])), C.int(len(cDfs)), cRechunk)
	if ptr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(ptr)}
}

// ConcatCols horizontally concatenates DataFrames. Row counts must match.
// If rechunk is true, the result is consolidated into a single chunk.
func ConcatCols(rechunk bool, dfs ...*DataFrame) *DataFrame {
	if len(dfs) == 0 {
		log.Println("error: no dataframes provided to ConcatCols")
		return &DataFrame{}
	}

	cDfs := make([]*C.CDataFrame, len(dfs))
	for i, d := range dfs {
		if d == nil || d.ptr == nil {
			log.Println("error: ConcatCols encountered nil dataframe")
			return &DataFrame{}
		}
		cDfs[i] = d.ptr
	}

	var cRechunk C.uint8_t
	if rechunk {
		cRechunk = 1
	}

	ptr := C.concat_df_horizontal((**C.CDataFrame)(unsafe.Pointer(&cDfs[0])), C.int(len(cDfs)), cRechunk)
	if ptr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(ptr)}
}

// DataFrameBuilder provides a fluent API for building DataFrames with mixed column types.
type DataFrameBuilder struct {
	columns  []columnSpec
	rowCount int
	hasRows  bool
	err      error
}

type columnSpec struct {
	name       string
	columnType C.CColumnType
	data       interface{}
	length     int
	subLengths []int  // per-row lengths for list columns
	validity   []bool // optional per-row validity (nil => all valid)
}

// NewDataFrame creates a new DataFrameBuilder.
func NewDataFrame() *DataFrameBuilder {
	return &DataFrameBuilder{
		columns: make([]columnSpec, 0),
	}
}

// DataFrameFromMap builds a DataFrame from a map of column name to slice.
// Supported slice types: []string, []int64, []int32, []int, []float64, []float32, []bool, []time.Time (as Date, days since epoch), and list columns: [][]string/[][]int64/[][]float64/[][]bool.
// All columns must have the same length; otherwise returns an error.
func DataFrameFromMap(cols map[string]interface{}) (*DataFrame, error) {
	if len(cols) == 0 {
		return nil, errors.New("no columns provided")
	}

	// Stable order
	keys := make([]string, 0, len(cols))
	for k := range cols {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var expectedLen int = -1
	b := NewDataFrame()

	for _, name := range keys {
		col := cols[name]
		switch v := col.(type) {
		case []string:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddStringColumn(name, v)

		case []int64:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddIntColumn(name, v)

		case []int32:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			vals := make([]int64, len(v))
			for i, vv := range v {
				vals[i] = int64(vv)
			}
			b.AddIntColumn(name, vals)

		case []int:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			vals := make([]int64, len(v))
			for i, vv := range v {
				vals[i] = int64(vv)
			}
			b.AddIntColumn(name, vals)

		case []float64:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddFloatColumn(name, v)

		case []float32:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			vals := make([]float64, len(v))
			for i, vv := range v {
				vals[i] = float64(vv)
			}
			b.AddFloatColumn(name, vals)

		case []bool:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddBoolColumn(name, v)

		case [][]string:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddListStringColumn(name, v)

		case [][]int64:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddListIntColumn(name, v)

		case [][]float64:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddListFloatColumn(name, v)

		case [][]bool:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			b.AddListBoolColumn(name, v)

		case []time.Time:
			expectedLen = ensureLen(expectedLen, len(v))
			if expectedLen < 0 {
				return nil, fmt.Errorf("column %s length mismatch", name)
			}
			days := make([]int32, len(v))
			for i, t := range v {
				days[i] = int32(t.UTC().Unix() / 86400)
			}
			b.AddDateColumn(name, days)

		default:
			return nil, fmt.Errorf("unsupported column type for %s", name)
		}
	}

	return b.Build()
}

func ensureLen(expected, current int) int {
	if expected == -1 {
		return current
	}
	if expected != current {
		return -1
	}
	return expected
}

// AddStringColumn adds a string column to the DataFrame.
func (b *DataFrameBuilder) AddStringColumn(name string, values []string) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b // Could store error for later, but keeping simple for now
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_STRING,
		data:       values,
		length:     len(values),
	})
	return b
}

// AddStringColumnNullable adds a string column with explicit validity (false => null).
func (b *DataFrameBuilder) AddStringColumnNullable(name string, values []string, validity []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}
	if len(validity) != 0 && len(validity) != len(values) {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_STRING,
		data:       values,
		length:     len(values),
		validity:   validity,
	})
	return b
}

// AddIntColumn adds an int64 column to the DataFrame.
func (b *DataFrameBuilder) AddIntColumn(name string, values []int64) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_INT64,
		data:       values,
		length:     len(values),
	})
	return b
}

// AddIntColumnNullable adds an int64 column with explicit validity (false => null).
func (b *DataFrameBuilder) AddIntColumnNullable(name string, values []int64, validity []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}
	if len(validity) != 0 && len(validity) != len(values) {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_INT64,
		data:       values,
		length:     len(values),
		validity:   validity,
	})
	return b
}

// AddFloatColumn adds a float64 column to the DataFrame.
func (b *DataFrameBuilder) AddFloatColumn(name string, values []float64) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_FLOAT64,
		data:       values,
		length:     len(values),
	})
	return b
}

// AddFloatColumnNullable adds a float64 column with explicit validity (false => null).
func (b *DataFrameBuilder) AddFloatColumnNullable(name string, values []float64, validity []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}
	if len(validity) != 0 && len(validity) != len(values) {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_FLOAT64,
		data:       values,
		length:     len(values),
		validity:   validity,
	})
	return b
}

// AddBoolColumn adds a boolean column to the DataFrame.
func (b *DataFrameBuilder) AddBoolColumn(name string, values []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_BOOL,
		data:       values,
		length:     len(values),
	})
	return b
}

// AddBoolColumnNullable adds a bool column with explicit validity (false => null).
func (b *DataFrameBuilder) AddBoolColumnNullable(name string, values []bool, validity []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}
	if len(validity) != 0 && len(validity) != len(values) {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_BOOL,
		data:       values,
		length:     len(values),
		validity:   validity,
	})
	return b
}

// AddDateColumn adds a Date column (days since Unix epoch, i32).
func (b *DataFrameBuilder) AddDateColumn(name string, values []int32) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_DATE,
		data:       values,
		length:     len(values),
	})
	return b
}

// AddDateColumnNullable adds a nullable Date column.
func (b *DataFrameBuilder) AddDateColumnNullable(name string, values []int32, validity []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}
	if len(validity) != 0 && len(validity) != len(values) {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_DATE,
		data:       values,
		length:     len(values),
		validity:   validity,
	})
	return b
}

// AddDatetimeMsColumn adds a Datetime(ms) column (i64 milliseconds since Unix epoch).
func (b *DataFrameBuilder) AddDatetimeMsColumn(name string, values []int64) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_DATETIME_MS,
		data:       values,
		length:     len(values),
	})
	return b
}

// AddDatetimeMsColumnNullable adds a nullable Datetime(ms) column.
func (b *DataFrameBuilder) AddDatetimeMsColumnNullable(name string, values []int64, validity []bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}
	if len(validity) != 0 && len(validity) != len(values) {
		return b
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_DATETIME_MS,
		data:       values,
		length:     len(values),
		validity:   validity,
	})
	return b
}

// AddListStringColumn adds a list<string> column to the DataFrame.
func (b *DataFrameBuilder) AddListStringColumn(name string, values [][]string) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	subLens := make([]int, len(values))
	for i, row := range values {
		subLens[i] = len(row)
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_LIST_STRING,
		data:       values,
		length:     len(values),
		subLengths: subLens,
	})
	return b
}

// AddListIntColumn adds a list<int64> column to the DataFrame.
func (b *DataFrameBuilder) AddListIntColumn(name string, values [][]int64) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	subLens := make([]int, len(values))
	for i, row := range values {
		subLens[i] = len(row)
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_LIST_INT64,
		data:       values,
		length:     len(values),
		subLengths: subLens,
	})
	return b
}

// AddListFloatColumn adds a list<float64> column to the DataFrame.
func (b *DataFrameBuilder) AddListFloatColumn(name string, values [][]float64) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	subLens := make([]int, len(values))
	for i, row := range values {
		subLens[i] = len(row)
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_LIST_FLOAT64,
		data:       values,
		length:     len(values),
		subLengths: subLens,
	})
	return b
}

// AddListBoolColumn adds a list<bool> column to the DataFrame.
func (b *DataFrameBuilder) AddListBoolColumn(name string, values [][]bool) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	subLens := make([]int, len(values))
	for i, row := range values {
		subLens[i] = len(row)
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_LIST_BOOL,
		data:       values,
		length:     len(values),
		subLengths: subLens,
	})
	return b
}

// AddListDatetimeMsColumn adds a list<datetime(ms)> column to the DataFrame.
func (b *DataFrameBuilder) AddListDatetimeMsColumn(name string, values [][]time.Time) *DataFrameBuilder {
	if err := b.validateColumnLength(len(values)); err != nil {
		return b
	}

	subLens := make([]int, len(values))
	for i, row := range values {
		subLens[i] = len(row)
	}

	b.columns = append(b.columns, columnSpec{
		name:       name,
		columnType: C.COLUMN_LIST_DATETIME_MS,
		data:       values,
		length:     len(values),
		subLengths: subLens,
	})
	return b
}

// SetListValidity sets per-row validity (true=valid, false=null) for a list column by name.
// If validity is nil, the column remains all-valid. Length must match column length.
func (b *DataFrameBuilder) SetListValidity(name string, validity []bool) *DataFrameBuilder {
	for idx := range b.columns {
		col := &b.columns[idx]
		if col.name != name {
			continue
		}
		switch col.columnType {
		case C.COLUMN_LIST_STRING, C.COLUMN_LIST_INT64, C.COLUMN_LIST_FLOAT64, C.COLUMN_LIST_BOOL, C.COLUMN_LIST_DATETIME_MS:
			if len(validity) != 0 && len(validity) != col.length {
				panic("SetListValidity: length mismatch with column")
			}
			col.validity = validity
		default:
			panic("SetListValidity: column is not a list type")
		}
	}
	return b
}

// validateColumnLength ensures all columns have the same length.
func (b *DataFrameBuilder) validateColumnLength(length int) error {
	if b.err != nil {
		return b.err
	}
	if !b.hasRows {
		b.rowCount = length
		b.hasRows = true
		return nil
	}

	if length != b.rowCount {
		b.err = fmt.Errorf("column length %d does not match expected length %d", length, b.rowCount)
		return b.err
	}

	return nil
}

// Build creates the DataFrame from the added columns.
func (b *DataFrameBuilder) Build() (*DataFrame, error) {
	if b.err != nil {
		return nil, b.err
	}
	if len(b.columns) == 0 {
		return nil, errors.New("no columns added to builder")
	}

	// Create C column specifications
	cSpecs := make([]C.CColumnSpec, len(b.columns))
	var managedMemory []unsafe.Pointer

	defer func() {
		// Clean up all allocated memory
		for _, ptr := range managedMemory {
			C.free(ptr)
		}
	}()

	for i, col := range b.columns {
		// Set column name
		cName := C.CString(col.name)
		managedMemory = append(managedMemory, unsafe.Pointer(cName))

		cSpecs[i].name = cName
		cSpecs[i].column_type = col.columnType
		cSpecs[i].length = C.int(col.length)
		cSpecs[i].sub_lengths = nil
		cSpecs[i].validity = nil

		// Handle data based on type
		switch col.columnType {
		case C.COLUMN_STRING:
			values := col.data.([]string)
			if len(values) == 0 {
				cSpecs[i].data = nil
			} else {
				// Create array of C string pointers
				cStringPtrs := (*C.char)(C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(uintptr(0)))))
				managedMemory = append(managedMemory, unsafe.Pointer(cStringPtrs))

				cStringArray := (*[1 << 30]*C.char)(unsafe.Pointer(cStringPtrs))[:len(values):len(values)]
				for j, str := range values {
					cStr := C.CString(str)
					managedMemory = append(managedMemory, unsafe.Pointer(cStr))
					cStringArray[j] = cStr
				}
				cSpecs[i].data = unsafe.Pointer(cStringPtrs)
			}

			if col.validity != nil {
				validityPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
				managedMemory = append(managedMemory, validityPtr)
				validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(values):len(values)]
				for j, v := range col.validity {
					if v {
						validitySlice[j] = 1
					} else {
						validitySlice[j] = 0
					}
				}
				cSpecs[i].validity = (*C.uchar)(validityPtr)
			}

		case C.COLUMN_INT64:
			values := col.data.([]int64)
			if len(values) == 0 {
				cSpecs[i].data = nil
			} else {
				cIntData := (*C.longlong)(C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.longlong(0)))))
				managedMemory = append(managedMemory, unsafe.Pointer(cIntData))

				cIntArray := (*[1 << 30]C.longlong)(unsafe.Pointer(cIntData))[:len(values):len(values)]
				for j, val := range values {
					cIntArray[j] = C.longlong(val)
				}
				cSpecs[i].data = unsafe.Pointer(cIntData)
			}

			if col.validity != nil {
				validityPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
				managedMemory = append(managedMemory, validityPtr)
				validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(values):len(values)]
				for j, v := range col.validity {
					if v {
						validitySlice[j] = 1
					} else {
						validitySlice[j] = 0
					}
				}
				cSpecs[i].validity = (*C.uchar)(validityPtr)
			}

		case C.COLUMN_FLOAT64:
			values := col.data.([]float64)
			if len(values) == 0 {
				cSpecs[i].data = nil
			} else {
				cFloatData := (*C.double)(C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.double(0)))))
				managedMemory = append(managedMemory, unsafe.Pointer(cFloatData))

				cFloatArray := (*[1 << 30]C.double)(unsafe.Pointer(cFloatData))[:len(values):len(values)]
				for j, val := range values {
					cFloatArray[j] = C.double(val)
				}
				cSpecs[i].data = unsafe.Pointer(cFloatData)
			}

			if col.validity != nil {
				validityPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
				managedMemory = append(managedMemory, validityPtr)
				validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(values):len(values)]
				for j, v := range col.validity {
					if v {
						validitySlice[j] = 1
					} else {
						validitySlice[j] = 0
					}
				}
				cSpecs[i].validity = (*C.uchar)(validityPtr)
			}

		case C.COLUMN_BOOL:
			values := col.data.([]bool)
			if len(values) == 0 {
				cSpecs[i].data = nil
			} else {
				cBoolData := (*C.uchar)(C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0)))))
				managedMemory = append(managedMemory, unsafe.Pointer(cBoolData))

				cBoolArray := (*[1 << 30]C.uchar)(unsafe.Pointer(cBoolData))[:len(values):len(values)]
				for j, val := range values {
					if val {
						cBoolArray[j] = 1
					} else {
						cBoolArray[j] = 0
					}
				}
				cSpecs[i].data = unsafe.Pointer(cBoolData)
			}

			if col.validity != nil {
				validityPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
				managedMemory = append(managedMemory, validityPtr)
				validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(values):len(values)]
				for j, v := range col.validity {
					if v {
						validitySlice[j] = 1
					} else {
						validitySlice[j] = 0
					}
				}
				cSpecs[i].validity = (*C.uchar)(validityPtr)
			}

		case C.COLUMN_DATE:
			values := col.data.([]int32)
			if len(values) == 0 {
				cSpecs[i].data = nil
			} else {
				cDateData := (*C.int)(C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.int(0)))))
				managedMemory = append(managedMemory, unsafe.Pointer(cDateData))

				cDateArray := (*[1 << 30]C.int)(unsafe.Pointer(cDateData))[:len(values):len(values)]
				for j, val := range values {
					cDateArray[j] = C.int(val)
				}
				cSpecs[i].data = unsafe.Pointer(cDateData)
			}

			if col.validity != nil {
				validityPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
				managedMemory = append(managedMemory, validityPtr)
				validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(values):len(values)]
				for j, v := range col.validity {
					if v {
						validitySlice[j] = 1
					} else {
						validitySlice[j] = 0
					}
				}
				cSpecs[i].validity = (*C.uchar)(validityPtr)
			}

		case C.COLUMN_DATETIME_MS:
			values := col.data.([]int64)
			if len(values) == 0 {
				cSpecs[i].data = nil
			} else {
				cDtData := (*C.longlong)(C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.longlong(0)))))
				managedMemory = append(managedMemory, unsafe.Pointer(cDtData))

				cDtArray := (*[1 << 30]C.longlong)(unsafe.Pointer(cDtData))[:len(values):len(values)]
				for j, val := range values {
					cDtArray[j] = C.longlong(val)
				}
				cSpecs[i].data = unsafe.Pointer(cDtData)
			}

			if col.validity != nil {
				validityPtr := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
				managedMemory = append(managedMemory, validityPtr)
				validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(values):len(values)]
				for j, v := range col.validity {
					if v {
						validitySlice[j] = 1
					} else {
						validitySlice[j] = 0
					}
				}
				cSpecs[i].validity = (*C.uchar)(validityPtr)
			}
		case C.COLUMN_LIST_STRING:
			rows := col.data.([][]string)
			if len(rows) == 0 {
				cSpecs[i].data = nil
			} else {
				lens := col.subLengths
				if len(lens) == 0 {
					cSpecs[i].data = nil
					cSpecs[i].sub_lengths = nil
					break
				}
				cLensSize := C.size_t(len(lens)) * C.size_t(unsafe.Sizeof(C.int(0)))
				cLensPtr := C.malloc(cLensSize)
				managedMemory = append(managedMemory, cLensPtr)
				cLens := (*[1 << 30]C.int)(cLensPtr)[:len(lens):len(lens)]
				for j, l := range lens {
					cLens[j] = C.int(l)
				}

				rowPtrsSize := C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(uintptr(0)))
				rowPtrs := C.malloc(rowPtrsSize)
				managedMemory = append(managedMemory, rowPtrs)
				rowPtrsSlice := (*[1 << 30]**C.char)(rowPtrs)[:len(rows):len(rows)]

				for j, row := range rows {
					if len(row) == 0 {
						rowPtrsSlice[j] = nil
						continue
					}
					cRowSize := C.size_t(len(row)) * C.size_t(unsafe.Sizeof(uintptr(0)))
					cRowPtr := C.malloc(cRowSize)
					managedMemory = append(managedMemory, cRowPtr)
					cRow := (*[1 << 30]*C.char)(cRowPtr)[:len(row):len(row)]
					for k, s := range row {
						cStr := C.CString(s)
						managedMemory = append(managedMemory, unsafe.Pointer(cStr))
						cRow[k] = cStr
					}
					rowPtrsSlice[j] = (**C.char)(cRowPtr)
				}

				cSpecs[i].data = rowPtrs
				cSpecs[i].sub_lengths = (*C.int)(cLensPtr)

				if col.validity != nil {
					validityPtr := C.malloc(C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
					managedMemory = append(managedMemory, validityPtr)
					validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(rows):len(rows)]
					for j, v := range col.validity {
						if v {
							validitySlice[j] = 1
						} else {
							validitySlice[j] = 0
						}
					}
					cSpecs[i].validity = (*C.uchar)(validityPtr)
				}
			}

		case C.COLUMN_LIST_INT64:
			rows := col.data.([][]int64)
			if len(rows) == 0 {
				cSpecs[i].data = nil
			} else {
				lens := col.subLengths
				if len(lens) == 0 {
					cSpecs[i].data = nil
					cSpecs[i].sub_lengths = nil
					break
				}
				cLensSize := C.size_t(len(lens)) * C.size_t(unsafe.Sizeof(C.int(0)))
				cLensPtr := C.malloc(cLensSize)
				managedMemory = append(managedMemory, cLensPtr)
				cLens := (*[1 << 30]C.int)(cLensPtr)[:len(lens):len(lens)]
				for j, l := range lens {
					cLens[j] = C.int(l)
				}

				rowPtrsSize := C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(uintptr(0)))
				rowPtrs := C.malloc(rowPtrsSize)
				managedMemory = append(managedMemory, rowPtrs)
				rowPtrsSlice := (*[1 << 30]*C.longlong)(rowPtrs)[:len(rows):len(rows)]

				for j, row := range rows {
					if len(row) == 0 {
						rowPtrsSlice[j] = nil
						continue
					}
					cRowSize := C.size_t(len(row)) * C.size_t(unsafe.Sizeof(C.longlong(0)))
					cRowPtr := C.malloc(cRowSize)
					managedMemory = append(managedMemory, cRowPtr)
					cRow := (*[1 << 30]C.longlong)(cRowPtr)[:len(row):len(row)]
					for k, v := range row {
						cRow[k] = C.longlong(v)
					}
					rowPtrsSlice[j] = (*C.longlong)(cRowPtr)
				}

				cSpecs[i].data = rowPtrs
				cSpecs[i].sub_lengths = (*C.int)(cLensPtr)

				if col.validity != nil {
					validityPtr := C.malloc(C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
					managedMemory = append(managedMemory, validityPtr)
					validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(rows):len(rows)]
					for j, v := range col.validity {
						if v {
							validitySlice[j] = 1
						} else {
							validitySlice[j] = 0
						}
					}
					cSpecs[i].validity = (*C.uchar)(validityPtr)
				}
			}

		case C.COLUMN_LIST_FLOAT64:
			rows := col.data.([][]float64)
			if len(rows) == 0 {
				cSpecs[i].data = nil
			} else {
				lens := col.subLengths
				if len(lens) == 0 {
					cSpecs[i].data = nil
					cSpecs[i].sub_lengths = nil
					break
				}
				cLensSize := C.size_t(len(lens)) * C.size_t(unsafe.Sizeof(C.int(0)))
				cLensPtr := C.malloc(cLensSize)
				managedMemory = append(managedMemory, cLensPtr)
				cLens := (*[1 << 30]C.int)(cLensPtr)[:len(lens):len(lens)]
				for j, l := range lens {
					cLens[j] = C.int(l)
				}

				rowPtrsSize := C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(uintptr(0)))
				rowPtrs := C.malloc(rowPtrsSize)
				managedMemory = append(managedMemory, rowPtrs)
				rowPtrsSlice := (*[1 << 30]*C.double)(rowPtrs)[:len(rows):len(rows)]

				for j, row := range rows {
					if len(row) == 0 {
						rowPtrsSlice[j] = nil
						continue
					}
					cRowSize := C.size_t(len(row)) * C.size_t(unsafe.Sizeof(C.double(0)))
					cRowPtr := C.malloc(cRowSize)
					managedMemory = append(managedMemory, cRowPtr)
					cRow := (*[1 << 30]C.double)(cRowPtr)[:len(row):len(row)]
					for k, v := range row {
						cRow[k] = C.double(v)
					}
					rowPtrsSlice[j] = (*C.double)(cRowPtr)
				}

				cSpecs[i].data = rowPtrs
				cSpecs[i].sub_lengths = (*C.int)(cLensPtr)

				if col.validity != nil {
					validityPtr := C.malloc(C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
					managedMemory = append(managedMemory, validityPtr)
					validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(rows):len(rows)]
					for j, v := range col.validity {
						if v {
							validitySlice[j] = 1
						} else {
							validitySlice[j] = 0
						}
					}
					cSpecs[i].validity = (*C.uchar)(validityPtr)
				}
			}

		case C.COLUMN_LIST_BOOL:
			rows := col.data.([][]bool)
			if len(rows) == 0 {
				cSpecs[i].data = nil
			} else {
				lens := col.subLengths
				if len(lens) == 0 {
					cSpecs[i].data = nil
					cSpecs[i].sub_lengths = nil
					break
				}
				cLensSize := C.size_t(len(lens)) * C.size_t(unsafe.Sizeof(C.int(0)))
				cLensPtr := C.malloc(cLensSize)
				managedMemory = append(managedMemory, cLensPtr)
				cLens := (*[1 << 30]C.int)(cLensPtr)[:len(lens):len(lens)]
				for j, l := range lens {
					cLens[j] = C.int(l)
				}

				rowPtrsSize := C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(uintptr(0)))
				rowPtrs := C.malloc(rowPtrsSize)
				managedMemory = append(managedMemory, rowPtrs)
				rowPtrsSlice := (*[1 << 30]*C.uchar)(rowPtrs)[:len(rows):len(rows)]

				for j, row := range rows {
					if len(row) == 0 {
						rowPtrsSlice[j] = nil
						continue
					}
					cRowSize := C.size_t(len(row)) * C.size_t(unsafe.Sizeof(C.uchar(0)))
					cRowPtr := C.malloc(cRowSize)
					managedMemory = append(managedMemory, cRowPtr)
					cRow := (*[1 << 30]C.uchar)(cRowPtr)[:len(row):len(row)]
					for k, v := range row {
						if v {
							cRow[k] = 1
						} else {
							cRow[k] = 0
						}
					}
					rowPtrsSlice[j] = (*C.uchar)(cRowPtr)
				}

				cSpecs[i].data = rowPtrs
				cSpecs[i].sub_lengths = (*C.int)(cLensPtr)

				if col.validity != nil {
					validityPtr := C.malloc(C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
					managedMemory = append(managedMemory, validityPtr)
					validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(rows):len(rows)]
					for j, v := range col.validity {
						if v {
							validitySlice[j] = 1
						} else {
							validitySlice[j] = 0
						}
					}
					cSpecs[i].validity = (*C.uchar)(validityPtr)
				}
			}

		case C.COLUMN_LIST_DATETIME_MS:
			rows := col.data.([][]time.Time)
			if len(rows) == 0 {
				cSpecs[i].data = nil
			} else {
				lens := col.subLengths
				if len(lens) == 0 {
					cSpecs[i].data = nil
					cSpecs[i].sub_lengths = nil
					break
				}
				cLensSize := C.size_t(len(lens)) * C.size_t(unsafe.Sizeof(C.int(0)))
				cLensPtr := C.malloc(cLensSize)
				managedMemory = append(managedMemory, cLensPtr)
				cLens := (*[1 << 30]C.int)(cLensPtr)[:len(lens):len(lens)]
				for j, l := range lens {
					cLens[j] = C.int(l)
				}

				rowPtrsSize := C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(uintptr(0)))
				rowPtrs := C.malloc(rowPtrsSize)
				managedMemory = append(managedMemory, rowPtrs)
				rowPtrsSlice := (*[1 << 30]*C.longlong)(rowPtrs)[:len(rows):len(rows)]

				for j, row := range rows {
					if len(row) == 0 {
						rowPtrsSlice[j] = nil
						continue
					}
					cRowSize := C.size_t(len(row)) * C.size_t(unsafe.Sizeof(C.longlong(0)))
					cRowPtr := C.malloc(cRowSize)
					managedMemory = append(managedMemory, cRowPtr)
					cRow := (*[1 << 30]C.longlong)(cRowPtr)[:len(row):len(row)]
					for k, v := range row {
						cRow[k] = C.longlong(v.UnixMilli())
					}
					rowPtrsSlice[j] = (*C.longlong)(cRowPtr)
				}

				cSpecs[i].data = rowPtrs
				cSpecs[i].sub_lengths = (*C.int)(cLensPtr)

				if col.validity != nil {
					validityPtr := C.malloc(C.size_t(len(rows)) * C.size_t(unsafe.Sizeof(C.uchar(0))))
					managedMemory = append(managedMemory, validityPtr)
					validitySlice := (*[1 << 30]C.uchar)(validityPtr)[:len(rows):len(rows)]
					for j, v := range col.validity {
						if v {
							validitySlice[j] = 1
						} else {
							validitySlice[j] = 0
						}
					}
					cSpecs[i].validity = (*C.uchar)(validityPtr)
				}
			}

		}
	}

	// Call the C function
	dfPtr := C.create_dataframe_mixed(
		(*C.CColumnSpec)(unsafe.Pointer(&cSpecs[0])),
		C.int(len(cSpecs)),
	)

	if dfPtr == nil {
		err := errors.New(C.GoString(C.get_last_error_message()))
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	return &DataFrame{ptr: (*C.CDataFrame)(dfPtr)}, nil
}

// SortBy sorts the DataFrame by one or more columns with specified sort orders.
func (df *DataFrame) SortBy(columns []string, descending []bool) *DataFrame {
	if df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &DataFrame{}
	}

	if len(columns) != len(descending) {
		log.Println("error: columns and descending arrays must have the same length")
		return &DataFrame{}
	}

	// Join column names with comma separator
	columnsStr := ""
	for i, col := range columns {
		if i > 0 {
			columnsStr += ","
		}
		columnsStr += col
	}

	cColumns := C.CString(columnsStr)
	defer C.free(unsafe.Pointer(cColumns))

	// Build descending string
	descendingStr := ""
	for i, desc := range descending {
		if i > 0 {
			descendingStr += ","
		}
		if desc {
			descendingStr += "true"
		} else {
			descendingStr += "false"
		}
	}

	cDescending := C.CString(descendingStr)
	defer C.free(unsafe.Pointer(cDescending))

	sortedPtr := C.sort_by_columns(df.ptr, cColumns, cDescending)
	if sortedPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(sortedPtr)}
}

// SortByExprs sorts the DataFrame by expressions with specified sort orders.
func (df *DataFrame) SortByExprs(exprs []Expr, descending []bool) *DataFrame {
	if df.ptr == nil {
		log.Println("error: DataFrame is nil")
		return &DataFrame{}
	}

	if len(exprs) != len(descending) {
		log.Println("error: exprs and descending arrays must have the same length")
		return &DataFrame{}
	}

	cExprs := make([]*C.CExpr, len(exprs))
	for i, expr := range exprs {
		cExprs[i] = expr.ptr
	}

	cExprsPtr := (**C.CExpr)(unsafe.Pointer(&cExprs[0]))
	cExprsLen := C.int(len(exprs))

	// Build descending string
	descendingStr := ""
	for i, desc := range descending {
		if i > 0 {
			descendingStr += ","
		}
		if desc {
			descendingStr += "true"
		} else {
			descendingStr += "false"
		}
	}

	cDescending := C.CString(descendingStr)
	defer C.free(unsafe.Pointer(cDescending))

	sortedPtr := C.sort_by_exprs(df.ptr, cExprsPtr, cExprsLen, cDescending)
	if sortedPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error_message())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(sortedPtr)}
}

// DataType mirrors the Polars logical types exposed over FFI.
type DataType int

const (
	DataTypeInvalid    DataType = -1
	DataTypeBoolean    DataType = 0
	DataTypeInt32      DataType = 1
	DataTypeInt64      DataType = 2
	DataTypeFloat32    DataType = 3
	DataTypeFloat64    DataType = 4
	DataTypeUTF8       DataType = 5
	DataTypeDate       DataType = 6
	DataTypeDatetimeMs DataType = 7
)

func (dt DataType) String() string {
	switch dt {
	case DataTypeBoolean:
		return "Boolean"
	case DataTypeInt32:
		return "Int32"
	case DataTypeInt64:
		return "Int64"
	case DataTypeFloat32:
		return "Float32"
	case DataTypeFloat64:
		return "Float64"
	case DataTypeUTF8:
		return "Utf8"
	case DataTypeDate:
		return "Date"
	case DataTypeDatetimeMs:
		return "Datetime(Milliseconds)"
	default:
		return "Invalid"
	}
}

type ColumnSchema struct {
	Name string
	Type DataType
}

// Schema returns the schema of the DataFrame as a slice of ColumnSchema.
func (df *DataFrame) Schema() []ColumnSchema {
	if df == nil || df.ptr == nil || df.ptr.handle == nil {
		return nil
	}

	width := df.Width()
	schema := make([]ColumnSchema, 0, width)
	for i := 0; i < width; i++ {
		namePtr := C.dataframe_column_name(df.ptr, C.size_t(i))
		if namePtr == nil {
			break
		}
		name := C.GoString(namePtr)
		C.free(unsafe.Pointer(namePtr))

		typeTag := C.dataframe_column_dtype(df.ptr, C.size_t(i))
		if typeTag == -1 {
			continue
		}

		schema = append(schema, ColumnSchema{
			Name: name,
			Type: DataType(typeTag),
		})
	}

	return schema
}

// Cast casts the expression to the specified data type.
func (e Expr) Cast(dt DataType) Expr {
	if e.ptr == nil {
		return Expr{}
	}

	casted := C.expr_cast(e.ptr, C.int(dt))
	if casted == nil {
		err := C.GoString(C.get_last_error_message())
		if err != "" {
			log.Printf("cast error: %s", err)
		}
		return Expr{}
	}

	return Expr{ptr: (*C.CExpr)(casted)}
}

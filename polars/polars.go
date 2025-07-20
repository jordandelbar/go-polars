//go:generate go run setup_binary.go

package polars

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo linux LDFLAGS: -L/tmp/go-polars -L${SRCDIR}/bin -lpolars_go -Wl,-rpath=/tmp/go-polars
#cgo darwin LDFLAGS: -L/tmp/go-polars -L${SRCDIR}/bin -lpolars_go -Wl,-rpath,/tmp/go-polars
#cgo windows LDFLAGS: -L%TEMP%/go-polars -L${SRCDIR}/bin -lpolars_go
#include "polars_go.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
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

// GroupBy represents a Polars GroupBy operation.
type GroupBy struct {
	ptr *C.CGroupBy
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
	return int(C.dataframe_width(df.ptr))
}

// Height returns the number of rows in the DataFrame.
func (df *DataFrame) Height() int {
	return int(C.dataframe_height(df.ptr))
}

// Columns returns a list of column names in the DataFrame.
func (df *DataFrame) Columns() []string {
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
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.long(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.long(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.long(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_gt_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_gt_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.long(intVal)))}
	default:
		panic("Gt: unsupported value type")
	}
}

// Lt creates a "less than" expression.
func (e Expr) Lt(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.long(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.long(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.long(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_lt_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_lt_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		return Expr{ptr: (*C.CExpr)(C.col_lt(e.ptr, C.long(intVal)))}
	default:
		panic("Lt: unsupported value type")
	}
}

// Eq creates an "equal to" expression.
func (e Expr) Eq(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.long(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.long(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.long(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_eq_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_eq_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		return Expr{ptr: (*C.CExpr)(C.col_eq(e.ptr, C.long(intVal)))}
	default:
		panic("Eq: unsupported value type")
	}
}

// Ne creates a "not equal to" expression.
func (e Expr) Ne(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.long(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.long(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.long(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_ne_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_ne_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		return Expr{ptr: (*C.CExpr)(C.col_ne(e.ptr, C.long(intVal)))}
	default:
		panic("Ne: unsupported value type")
	}
}

// Ge creates a "greater than or equal to" expression.
func (e Expr) Ge(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.long(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.long(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.long(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_ge_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_ge_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		return Expr{ptr: (*C.CExpr)(C.col_ge(e.ptr, C.long(intVal)))}
	default:
		panic("Ge: unsupported value type")
	}
}

// Le creates a "less than or equal to" expression.
func (e Expr) Le(value interface{}) Expr {
	switch v := value.(type) {
	case int:
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.long(v)))}
	case int32:
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.long(v)))}
	case int64:
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.long(v)))}
	case float32:
		return Expr{ptr: (*C.CExpr)(C.col_le_f64(e.ptr, C.double(v)))}
	case float64:
		return Expr{ptr: (*C.CExpr)(C.col_le_f64(e.ptr, C.double(v)))}
	case bool:
		var intVal int64
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		return Expr{ptr: (*C.CExpr)(C.col_le(e.ptr, C.long(intVal)))}
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
		cExpr = C.lit_int64(C.long(v))
	case int32:
		cExpr = C.lit_int32(C.int(v))
	case int:
		cExpr = C.lit_int64(C.long(v)) // Treat as int64
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

// DataFrameBuilder provides a fluent API for building DataFrames with mixed column types.
type DataFrameBuilder struct {
	columns  []columnSpec
	rowCount int
	hasRows  bool
}

type columnSpec struct {
	name       string
	columnType C.CColumnType
	data       interface{}
	length     int
}

// NewDataFrameBuilder creates a new DataFrameBuilder.
func NewDataFrameBuilder() *DataFrameBuilder {
	return &DataFrameBuilder{
		columns: make([]columnSpec, 0),
	}
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

// validateColumnLength ensures all columns have the same length.
func (b *DataFrameBuilder) validateColumnLength(length int) error {
	if !b.hasRows {
		b.rowCount = length
		b.hasRows = true
		return nil
	}

	if length != b.rowCount {
		return fmt.Errorf("column length %d does not match expected length %d", length, b.rowCount)
	}

	return nil
}

// Build creates the DataFrame from the added columns.
func (b *DataFrameBuilder) Build() (*DataFrame, error) {
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

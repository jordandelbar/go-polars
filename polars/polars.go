package polars

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR}/bin -lpolars_go
#cgo linux LDFLAGS: -Wl,-rpath=${SRCDIR}/bin
#cgo darwin LDFLAGS: -Wl,-rpath,${SRCDIR}/bin
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

// Col creates a new expression representing a column.
func Col(name string) Expr {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return Expr{ptr: (*C.CExpr)(C.col(cName))}
}

// Gt creates a "greater than" expression.
func (e Expr) Gt(value int64) Expr {
	return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.long(value)))}
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

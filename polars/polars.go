package polars

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR}/lib -lpolars_go
#cgo linux LDFLAGS: -Wl,-rpath=${SRCDIR}/lib
#cgo darwin LDFLAGS: -Wl,-rpath,${SRCDIR}/lib
#include "polars_go.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"unsafe"
)

func init() {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Println("Failed to get executable path:", err)
		return
	}
	libPath := filepath.Join(filepath.Dir(exePath), "polars/lib")

	err = os.Setenv("LD_LIBRARY_PATH", libPath+":"+os.Getenv("LD_LIBRARY_PATH"))
	if err != nil {
		fmt.Println("Failed to set LD_LIBRARY_PATH:", err)
	}
}

type DataFrame struct {
	ptr *C.CDataFrame
}

type Expr struct {
	ptr *C.CExpr
}

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

func ReadCSV(filePath string) (*DataFrame, error) {
	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))

	df := C.read_csv(cPath)
	if df == nil || (*C.CDataFrame)(df).handle == nil {
		return nil, errors.New(C.GoString(C.get_last_error()))
	}

	return &DataFrame{ptr: (*C.CDataFrame)(df)}, nil
}

func (df *DataFrame) Free() {
	if df.ptr != nil {
		C.free_dataframe(df.ptr)
		df.ptr = nil
	}
}

func (df *DataFrame) Width() int {
	return int(C.dataframe_width(df.ptr))
}

func (df *DataFrame) Height() int {
	return int(C.dataframe_height(df.ptr))
}

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

func (df *DataFrame) Filter(expr Expr) *DataFrame {
	filteredPtr := C.filter(df.ptr, expr.ptr)
	if filteredPtr == nil {
		err := errors.New(C.GoString(C.get_last_error()))
		log.Printf("Error while filtering: %s", err)
		return &DataFrame{}
	}
	return &DataFrame{ptr: (*C.CDataFrame)(filteredPtr)}
}

func Col(name string) Expr {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return Expr{ptr: (*C.CExpr)(C.col(cName))}
}

func (e Expr) Gt(value int64) Expr {
	return Expr{ptr: (*C.CExpr)(C.col_gt(e.ptr, C.long(value)))}
}

func (df DataFrame) WriteCSV(filePath string) error {
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

func (df DataFrame) Head(n int) *DataFrame {
	cHeadDf := C.head(df.ptr, C.size_t(n))

	if cHeadDf == nil || (*C.CDataFrame)(cHeadDf).handle == nil {
		err := C.GoString(C.get_last_error())
		log.Printf("Error getting head: %s", err)
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(cHeadDf)}
}

func (df *DataFrame) WithColumns(exprs ...Expr) *DataFrame {
	cExprs := make([]*C.CExpr, len(exprs))
	for i, expr := range exprs {
		cExprs[i] = expr.ptr
	}

	cExprsPtr := (**C.CExpr)(unsafe.Pointer(&cExprs[0]))
	cExprsLen := C.int(len(exprs))

	newDfPtr := C.with_columns(df.ptr, cExprsPtr, cExprsLen)

	if newDfPtr == nil {
		log.Printf("error: %s", errors.New(C.GoString(C.get_last_error())))
		return &DataFrame{}
	}

	return &DataFrame{ptr: (*C.CDataFrame)(newDfPtr)}
}

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
		// Handle other types or return an error
		panic(fmt.Sprintf("Unsupported literal type: %T", value))
	}

	return Expr{ptr: (*C.CExpr)(cExpr)}
}

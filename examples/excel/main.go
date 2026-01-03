package main

import (
	"fmt"
	"log"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	df, err := pl.ReadExcelWithOptions("test.xlsx", pl.ExcelOptions{
		SheetName:        "专业录取数据",
		InferTypes:       true, // 开启类型推断
		TreatEmptyAsNull: true, // 空白转 null
	})
	if err != nil {
		log.Fatalf("failed to read excel: %v", err)
	}

	fmt.Println(df)
}

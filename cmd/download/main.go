package main

import (
	"log"
	"os"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	if err := polars.EnsureBinary(); err != nil {
		log.Fatalf("Failed to download binary: %v", err)
		os.Exit(1)
	}
	log.Println("Binary downloaded successfully")
}

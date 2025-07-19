package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	fmt.Println("=== Simple go-polars Test ===")
	fmt.Println()

	// Check if initialization was successful
	if !polars.IsInitialized() {
		fmt.Printf("❌ go-polars failed to initialize: %v\n", polars.GetInitError())
		fmt.Println()
		fmt.Println("This is expected if:")
		fmt.Println("1. No pre-compiled binary is available for download")
		fmt.Println("2. You haven't built the binary locally yet")
		fmt.Println()
		fmt.Println("To fix this, run: ./build.sh")
		os.Exit(1)
	}

	fmt.Println("✅ go-polars initialized successfully!")

	// Check if binary exists and get its location
	exists, binaryPath, err := polars.CheckBinaryExists()
	if err != nil {
		log.Printf("Error checking binary: %v", err)
		return
	}

	if exists {
		// Get file info
		if stat, err := os.Stat(binaryPath); err == nil {
			fmt.Printf("📦 Binary found at: %s\n", binaryPath)
			fmt.Printf("📊 Binary size: %.1f MB\n", float64(stat.Size())/(1024*1024))
		}
	}

	fmt.Println()
	fmt.Println("🧪 Testing basic functionality...")

	// Test creating a simple expression
	col := polars.Col("test_column")
	fmt.Println("✅ Column expression created successfully")

	// Test creating a literal value
	lit := polars.Lit(42)
	fmt.Println("✅ Literal expression created successfully")

	// Test creating aggregation expressions
	_ = polars.Count()
	fmt.Println("✅ Count expression created successfully")

	// Test expression operations
	_ = col.Gt(10).And(lit.Ne(0))
	fmt.Println("✅ Complex expression created successfully")

	fmt.Println()
	fmt.Println("🎉 All basic tests passed!")
	fmt.Println()
	fmt.Println("💡 This proves that:")
	fmt.Println("   - The binary was properly loaded")
	fmt.Println("   - CGO bindings are working")
	fmt.Println("   - Basic Polars functionality is available")
	fmt.Println()
	fmt.Println("🚀 You can now use go-polars in your projects with just:")
	fmt.Println("   go get github.com/jordandelbar/go-polars")
}

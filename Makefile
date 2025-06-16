.PHONY: local-build
local-build:
	@./build.sh

.PHONY: build
build: local-build

.PHONY: clean
clean:
	@echo "🧹 Cleaning build artifacts..."
	@cd polars/bindings && cargo clean
	@rm -f polars/bin/libpolars_go.so polars/bin/libpolars_go.dylib polars/bin/polars_go.dll
	@echo "✅ Clean completed!"

.PHONY: run-basic-example
run-basic-example:
	@cd examples/basic && go run .

.PHONY: run-expressions-example
run-expressions-example:
	@cd examples/expressions && go run .

.PHONY: test
test: local-build
	@echo "🧪 Running tests..."
	@cd tests && go test -v

.PHONY: test-short
test-short: local-build
	@echo "🧪 Running short tests..."
	@cd tests && go test -v -short

.PHONY: test-bench
test-bench: local-build
	@echo "📊 Running benchmarks..."
	@cd tests && go test -v -bench=.

.PHONY: test-coverage
test-coverage: local-build
	@echo "📈 Running tests with coverage..."
	@cd tests && go test -v -coverprofile=coverage.out
	@cd tests && go tool cover -html=coverage.out -o coverage.html
	@echo "📊 Coverage report generated: tests/coverage.html"

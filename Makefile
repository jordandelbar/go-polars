.PHONY: local-build
local-build:
	@./build.sh

.PHONY: build
build: local-build

.PHONY: force-build
force-build:
	@./build.sh --force

.PHONY: quick-build
quick-build:
	@./build.sh

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

.PHONY: run-groupby-example
run-groupby-example:
	@cd examples/groupby && go run .

.PHONY: run-all-examples
run-all-examples: run-basic-example run-expressions-example run-groupby-example
	@echo "✅ All examples completed!"

.PHONY: test
test: quick-build
	@echo "🧪 Running tests..."
	@cd tests && go test -v

.PHONY: test-short
test-short: quick-build
	@echo "🧪 Running short tests..."
	@cd tests && go test -v -short

.PHONY: test-bench
test-bench: quick-build
	@echo "📊 Running benchmarks..."
	@cd tests && go test -v -bench=.

.PHONY: test-coverage
test-coverage: quick-build
	@echo "📈 Running tests with coverage..."
	@cd tests && go test -v -coverprofile=coverage.out
	@cd tests && go tool cover -html=coverage.out -o coverage.html
	@echo "📊 Coverage report generated: tests/coverage.html"

.PHONY: test-groupby
test-groupby: quick-build
	@echo "🧪 Running GroupBy tests..."
	@cd tests && go test -v -run TestGroupBy

.PHONY: check-build
check-build:
	@echo "🔍 Checking if build is needed..."
	@./build.sh --check || echo "Build would be required"

.PHONY: dev
dev: quick-build test-short
	@echo "🚀 Development cycle complete!"

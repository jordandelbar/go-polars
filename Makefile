.PHONY: local-build
local-build:
	@cd polars/bindings && cargo build --release
	@cp polars/bindings/target/release/libpolars_go.so polars/bin/libpolars_go.so

.PHONY: run-basic-example
run-basic-example:
	@cd examples/basic && go run .

.PHONY: run-expressions-example
run-expressions-example:
	@cd examples/expressions && go run simple_example.go

.PHONY: run-expressions-demo
run-expressions-demo:
	@cd examples/expressions && go run .

.PHONY: local-build
local-build:
	@cd polars/bindings && cargo build --release
	@cp polars/bindings/target/release/libpolars_go.so polars/lib/

.PHONY: run-basic-example
run-basic-example:
	@cd examples/basic && go run .

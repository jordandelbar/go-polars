.PHONY: build
build:
	@cargo build --release
	@cp target/release/libpolars_go.so polars/lib/libpolars_go.so

.PHONY: run
run:
	@LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH go run .

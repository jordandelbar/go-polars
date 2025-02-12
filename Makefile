.PHONY: build
build:
	@cargo build --release

.PHONY: run
run:
	@LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH go run .

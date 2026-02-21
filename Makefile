.PHONY: build release check test lint fmt clean run

CARGO_ENV := source "$$HOME/.cargo/env" &&

build:
	$(CARGO_ENV) cargo build

release:
	$(CARGO_ENV) cargo build --release

check:
	$(CARGO_ENV) cargo check

test:
	$(CARGO_ENV) cargo test

lint:
	$(CARGO_ENV) cargo clippy -- -D warnings

fmt:
	$(CARGO_ENV) cargo fmt

fmt-check:
	$(CARGO_ENV) cargo fmt -- --check

clean:
	$(CARGO_ENV) cargo clean

run:
	$(CARGO_ENV) cargo run

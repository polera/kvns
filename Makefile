.DEFAULT_GOAL := help

.PHONY: help build release check checks audit tools test lint fmt fmt-check clean run benchmark benchmark-sharded benchmark-compare \
	podman-build podman-run podman-compose-up podman-compose-down podman-compose-logs \
	podman-login-ghcr podman-push-ghcr

CARGO_ENV := (source "$$HOME/.cargo/env" || true) &&
PODMAN ?= podman
IMAGE ?= kvns:local
COMPOSE_FILE ?= docker-compose.yaml
GHCR_IMAGE ?= ghcr.io/owner/repo
GHCR_USER ?= $(GITHUB_ACTOR)
TAG ?= latest
PLATFORMS ?= linux/amd64,linux/arm64
PUSH_LATEST ?= true
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

help: ## Show available commands
	@grep -E '^[a-zA-Z0-9][a-zA-Z0-9_.-]*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

build: ## Build with tests
	$(CARGO_ENV) cargo build --tests

release: ## Build release binary
	$(CARGO_ENV) cargo build --release
	cp target/release/kvns .

check: ## Run cargo check
	$(CARGO_ENV) cargo check --tests

tools: ## Install required tooling (clippy, cargo-audit)
	$(CARGO_ENV) rustup component add clippy
	@$(CARGO_ENV) command -v cargo-audit >/dev/null 2>&1 || cargo install --locked cargo-audit

checks: lint audit ## Run all checks (lint + audit)

audit: tools ## Run cargo audit
	$(CARGO_ENV) cargo audit

test: ## Run tests
	$(CARGO_ENV) cargo test

lint: ## Run clippy lints
	$(CARGO_ENV) cargo clippy --tests -- -D warnings

fmt: ## Format source code
	$(CARGO_ENV) cargo fmt

fmt-check: ## Check formatting without modifying
	$(CARGO_ENV) cargo fmt -- --check

clean: ## Remove build artifacts
	$(CARGO_ENV) cargo clean

run: ## Run the server
	$(CARGO_ENV) cargo run

benchmark: ## Run benchmark
	./scripts/benchmark_kvns.sh

benchmark-sharded: ## Run sharded benchmark
	BENCH_SHARDED_MODE=1 ./scripts/benchmark_kvns.sh

benchmark-compare: ## Run benchmark comparison (kvns vs redis)
	BENCH_COMPARE_BOTH=1 ./scripts/benchmark_kvns.sh

podman-build: ## Build container image
	$(PODMAN) build -f Dockerfile -t $(IMAGE) .

podman-run: ## Run container image
	$(PODMAN) run --rm --name kvns -p 6480:6480 -p 9090:9090 -v kvns-data:/data $(IMAGE)

podman-compose-up: ## Start services with podman compose
	$(PODMAN) compose -f $(COMPOSE_FILE) up -d --build

podman-compose-down: ## Stop services with podman compose
	$(PODMAN) compose -f $(COMPOSE_FILE) down

podman-compose-logs: ## Follow logs from podman compose
	$(PODMAN) compose -f $(COMPOSE_FILE) logs -f

podman-login-ghcr: ## Log in to GHCR
	@test -n "$(GHCR_USER)" || (echo "GHCR_USER is required" && exit 1)
	@test -n "$(GHCR_TOKEN)" || (echo "GHCR_TOKEN is required" && exit 1)
	@printf '%s' "$(GHCR_TOKEN)" | $(PODMAN) login ghcr.io -u "$(GHCR_USER)" --password-stdin

podman-push-ghcr: ## Build and push multi-arch image to GHCR
	@test -n "$(GHCR_IMAGE)" || (echo "GHCR_IMAGE is required" && exit 1)
	@test -n "$(TAG)" || (echo "TAG is required" && exit 1)
	$(PODMAN) manifest rm "$(GHCR_IMAGE):$(TAG)" >/dev/null 2>&1 || true
	$(PODMAN) build --platform $(PLATFORMS) --manifest "$(GHCR_IMAGE):$(TAG)" -f Dockerfile .
	$(PODMAN) manifest push --all "$(GHCR_IMAGE):$(TAG)" "docker://$(GHCR_IMAGE):$(TAG)"
	@if [ "$(PUSH_LATEST)" = "true" ] && [ "$(TAG)" != "latest" ]; then \
		$(PODMAN) manifest push --all "$(GHCR_IMAGE):$(TAG)" "docker://$(GHCR_IMAGE):latest"; \
	fi

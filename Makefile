.DEFAULT_GOAL := help

.PHONY: help build release check checks ensure-check-tools test lint fmt fmt-check clean run benchmark benchmark-sharded benchmark-compare \
	podman-build podman-run podman-compose-up podman-compose-down podman-compose-logs \
	podman-login-ghcr podman-push-ghcr

CARGO_ENV := source "$$HOME/.cargo/env" &&
PODMAN ?= podman
IMAGE ?= kvns:local
COMPOSE_FILE ?= docker-compose.yaml
GHCR_IMAGE ?= ghcr.io/owner/repo
GHCR_USER ?= $(GITHUB_ACTOR)
TAG ?= latest
PLATFORMS ?= linux/amd64,linux/arm64
PUSH_LATEST ?= true

help:
	@echo "Available targets:"
	@awk -F: '/^[a-zA-Z0-9][a-zA-Z0-9_.-]*:/{print " - " $$1}' Makefile

build:
	$(CARGO_ENV) cargo build --tests

release:
	$(CARGO_ENV) cargo build --tests --release

check:
	$(CARGO_ENV) cargo check --tests

ensure-check-tools:
	$(CARGO_ENV) if ! cargo clippy --version >/dev/null 2>&1; then \
		echo "clippy is not installed; bootstrapping via rustup..."; \
		rustup component add clippy; \
	fi
	$(CARGO_ENV) if ! command -v cargo-audit >/dev/null 2>&1; then \
		echo "cargo-audit is not installed; bootstrapping via cargo install..."; \
		cargo install --locked cargo-audit; \
	fi

checks: ensure-check-tools
	$(CARGO_ENV) cargo clippy --tests -- -D warnings
	$(CARGO_ENV) cargo audit

test:
	$(CARGO_ENV) cargo test

lint:
	$(CARGO_ENV) cargo clippy --tests -- -D warnings

fmt:
	$(CARGO_ENV) cargo fmt

fmt-check:
	$(CARGO_ENV) cargo fmt -- --check

clean:
	$(CARGO_ENV) cargo clean

run:
	$(CARGO_ENV) cargo run

benchmark:
	./scripts/benchmark_kvns_vs_dragonfly.sh

benchmark-sharded:
	BENCH_SHARDED_MODE=1 ./scripts/benchmark_kvns_vs_dragonfly.sh

benchmark-compare:
	BENCH_COMPARE_BOTH=1 ./scripts/benchmark_kvns_vs_dragonfly.sh

podman-build:
	$(PODMAN) build -f Dockerfile -t $(IMAGE) .

podman-run:
	$(PODMAN) run --rm --name kvns -p 6480:6480 -p 9090:9090 -v kvns-data:/data $(IMAGE)

podman-compose-up:
	$(PODMAN) compose -f $(COMPOSE_FILE) up -d --build

podman-compose-down:
	$(PODMAN) compose -f $(COMPOSE_FILE) down

podman-compose-logs:
	$(PODMAN) compose -f $(COMPOSE_FILE) logs -f

podman-login-ghcr:
	@test -n "$(GHCR_USER)" || (echo "GHCR_USER is required" && exit 1)
	@test -n "$(GHCR_TOKEN)" || (echo "GHCR_TOKEN is required" && exit 1)
	@printf '%s' "$(GHCR_TOKEN)" | $(PODMAN) login ghcr.io -u "$(GHCR_USER)" --password-stdin

podman-push-ghcr:
	@test -n "$(GHCR_IMAGE)" || (echo "GHCR_IMAGE is required" && exit 1)
	@test -n "$(TAG)" || (echo "TAG is required" && exit 1)
	$(PODMAN) manifest rm "$(GHCR_IMAGE):$(TAG)" >/dev/null 2>&1 || true
	$(PODMAN) build --platform $(PLATFORMS) --manifest "$(GHCR_IMAGE):$(TAG)" -f Dockerfile .
	$(PODMAN) manifest push --all "$(GHCR_IMAGE):$(TAG)" "docker://$(GHCR_IMAGE):$(TAG)"
	@if [ "$(PUSH_LATEST)" = "true" ] && [ "$(TAG)" != "latest" ]; then \
		$(PODMAN) manifest push --all "$(GHCR_IMAGE):$(TAG)" "docker://$(GHCR_IMAGE):latest"; \
	fi

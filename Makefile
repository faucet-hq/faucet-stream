# Convenience targets for running the faucet-stream examples.
# `make help` lists everything.

COMPOSE := docker compose -f examples/docker-compose.yml
# Slim feature set for the demo so it builds fast and needs no native libs
# (avoids the Kafka/librdkafka cmake dependency in the full build).
DEMO_FEATURES := source-file,sink-file,file-format-csv,sink-stdout,transforms
FAUCET_DEMO := cargo run -q -p faucet-cli --no-default-features --features "$(DEMO_FEATURES)" --

.DEFAULT_GOAL := help

.PHONY: help demo infra-up infra-down infra-logs infra-ps bench bench-smoke bench-postgres bench-build \
        image image-lean helm-lint helm-template

# --- container image (name-based connector selection; see Dockerfile header) ---
IMAGE_TAG    ?= faucet:local
IMAGE_SOURCES ?=
IMAGE_SINKS   ?=

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

demo: ## Run a no-infrastructure smoke test (CSV -> JSONL)
	@mkdir -p target/demo
	$(FAUCET_DEMO) validate examples/demo/pipeline.yaml
	$(FAUCET_DEMO) run examples/demo/pipeline.yaml
	@echo "--- target/demo/out.jsonl ---"
	@cat target/demo/out.jsonl

bench-build: ## Build the release faucet binary used by the benchmark harness
	cargo build -p faucet-cli --release --no-default-features --features "source-csv,sink-jsonl,source-postgres,sink-postgres"

bench: bench-build ## Run the Meltano comparison benchmark (1M rows, CSV->JSONL) — see BENCHMARKS.md
	scripts/run-bench.sh

bench-smoke: bench-build ## Fast benchmark validation (100k rows)
	scripts/run-bench.sh --smoke

bench-postgres: bench-build ## Run all scenarios incl. Postgres->Postgres (sink-bound); needs Docker
	scripts/run-bench.sh --postgres

image: ## Build the full container image (all connectors). Override IMAGE_TAG=...
	scripts/build-image.sh -t "$(IMAGE_TAG)"

image-lean: ## Build a lean image. e.g. make image-lean IMAGE_SOURCES=rest,postgres IMAGE_SINKS=s3,jsonl
	scripts/build-image.sh -t "$(IMAGE_TAG)" -s "$(IMAGE_SOURCES)" -k "$(IMAGE_SINKS)"

helm-lint: ## Lint the Helm chart
	helm lint deploy/helm/faucet-stream

helm-template: ## Render the Helm chart to stdout with default values
	helm template faucet deploy/helm/faucet-stream

infra-up: ## Start the local example stack (Postgres, MySQL, Kafka, Redis, Mongo, ES, MinIO)
	$(COMPOSE) up -d

infra-down: ## Stop the local stack and wipe its volumes
	$(COMPOSE) down -v

infra-ps: ## Show the status of the local stack
	$(COMPOSE) ps

infra-logs: ## Tail logs from the local stack
	$(COMPOSE) logs -f

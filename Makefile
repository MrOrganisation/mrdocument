.PHONY: build up down \
       build-service build-watcher build-stt build-ocrmypdf build-anthropic-adapter build-db \
       build-amd64 \
       push \
       release \
       test test-unit test-integration test-integration-syncthing \
       test-claude-code-adapter \
       peek-watcher peek-service peek-anthropic-adapter peek-stt \
       dump-db

# Disable BuildKit provenance attestation (stalls on some setups)
export BUILDX_NO_DEFAULT_ATTESTATIONS := 1

ENV_FILE ?= ../.env
COMPOSE := docker compose $(if $(wildcard $(ENV_FILE)),--env-file $(ENV_FILE),)

# ==============================================================================
# Versioning
# ==============================================================================

VERSION    := $(shell cat VERSION)
BRANCH     := $(shell git rev-parse --abbrev-ref HEAD)
COMMIT     := $(shell git rev-parse --short HEAD)
IMAGE_TAG  := $(VERSION)-$(BRANCH)-$(COMMIT)
REGISTRY   := 127.0.0.1:5000
RELEASE_IMAGES := mrdocument-service mrdocument-watcher stt ocrmypdf anthropic-adapter claude-code-adapter mrdocument-db

# ==============================================================================
# Build / Up / Down
# ==============================================================================

build: build-service build-watcher build-stt build-ocrmypdf build-anthropic-adapter build-db

build-service:
	$(COMPOSE) build mrdocument-service

build-watcher:
	$(COMPOSE) build mrdocument-watcher

build-stt:
	$(COMPOSE) build stt

build-ocrmypdf:
	$(COMPOSE) build ocrmypdf

build-anthropic-adapter:
	$(COMPOSE) build anthropic-adapter

build-db:
	$(COMPOSE) build mrdocument-db

CLAUDE_CODE_OVERRIDE := docker-compose.claude-code.yaml
COMPOSE_CLAUDE_CODE := $(COMPOSE) -f docker-compose.yaml -f $(CLAUDE_CODE_OVERRIDE)

up: build
	$(COMPOSE_CLAUDE_CODE) up -d

down:
	$(COMPOSE) down

# ==============================================================================
# Cross-build for linux/amd64 (from arm64 host via QEMU + buildx)
# ==============================================================================
# Requires: docker buildx with QEMU binfmt registered.
#   docker run --rm --privileged tonistiigi/binfmt --install amd64
#   docker buildx create --name multiarch --driver docker-container --use

BUILDX_BUILDER ?= multiarch
BUILDX_PLATFORM ?= linux/amd64

define buildx_image
	docker buildx build \
		--builder $(BUILDX_BUILDER) \
		--platform $(BUILDX_PLATFORM) \
		-f $(1) \
		-t $(2):latest-custom \
		--load \
		.
endef

build-amd64:
	$(call buildx_image,Dockerfile.service-rs,mrdocument-service)
	$(call buildx_image,Dockerfile.watcher,mrdocument-watcher)
	$(call buildx_image,Dockerfile.stt,stt)
	$(call buildx_image,Dockerfile.ocrmypdf,ocrmypdf)
	$(call buildx_image,Dockerfile.anthropic-adapter,anthropic-adapter)
	$(call buildx_image,Dockerfile.claude-code-adapter,claude-code-adapter)
	$(call buildx_image,Dockerfile.db,mrdocument-db)

define buildx_push
	docker buildx build \
		--builder $(BUILDX_BUILDER) \
		--platform $(BUILDX_PLATFORM) \
		-f $(1) \
		-t $(REGISTRY)/$(2):$(IMAGE_TAG) \
		-t $(REGISTRY)/$(2):latest-$(BRANCH) \
		--cache-from type=registry,ref=$(REGISTRY)/$(2):buildcache \
		--cache-to type=registry,ref=$(REGISTRY)/$(2):buildcache,mode=max \
		--push \
		.
endef

push-amd64:
	$(call buildx_push,Dockerfile.service-rs,mrdocument-service)
	$(call buildx_push,Dockerfile.watcher,mrdocument-watcher)
	$(call buildx_push,Dockerfile.stt,stt)
	$(call buildx_push,Dockerfile.ocrmypdf,ocrmypdf)
	$(call buildx_push,Dockerfile.anthropic-adapter,anthropic-adapter)
	$(call buildx_push,Dockerfile.claude-code-adapter,claude-code-adapter)
	$(call buildx_push,Dockerfile.db,mrdocument-db)

release-amd64:
	@for img in $(RELEASE_IMAGES); do \
		case $$img in \
			mrdocument-service) df=Dockerfile.service-rs ;; \
			mrdocument-watcher) df=Dockerfile.watcher ;; \
			mrdocument-db) df=Dockerfile.db ;; \
			*) df=Dockerfile.$$(echo $$img | sed 's/mrdocument-//') ;; \
		esac; \
		docker buildx build \
			--builder $(BUILDX_BUILDER) \
			--platform $(BUILDX_PLATFORM) \
			-f $$df \
			-t $(REGISTRY)/$$img:$(IMAGE_TAG) \
			-t $(REGISTRY)/$$img:latest-$(BRANCH) \
			-t $(REGISTRY)/$$img:latest \
			--cache-from type=registry,ref=$(REGISTRY)/$$img:buildcache \
			--cache-to type=registry,ref=$(REGISTRY)/$$img:buildcache,mode=max \
			--push \
			. ; \
	done

# ==============================================================================
# Push to registry (with layer cache)
# ==============================================================================

push:
	@for img in $(RELEASE_IMAGES); do \
		case $$img in \
			mrdocument-service) df=Dockerfile.service-rs ;; \
			mrdocument-watcher) df=Dockerfile.watcher ;; \
			mrdocument-db) df=Dockerfile.db ;; \
			*) df=Dockerfile.$$(echo $$img | sed 's/mrdocument-//') ;; \
		esac; \
		docker build \
			-f $$df \
			--build-arg GIT_COMMIT=$(COMMIT) \
			-t $(REGISTRY)/$$img:$(IMAGE_TAG) \
			-t $(REGISTRY)/$$img:latest-$(BRANCH) \
			. && \
		docker push $(REGISTRY)/$$img:$(IMAGE_TAG) && \
		docker push $(REGISTRY)/$$img:latest-$(BRANCH) ; \
	done

# ==============================================================================
# Release (retag latest-<branch> as latest, then push)
# ==============================================================================

release:
	@for img in $(RELEASE_IMAGES); do \
		docker pull $(REGISTRY)/$$img:latest-$(BRANCH); \
		docker tag $(REGISTRY)/$$img:latest-$(BRANCH) $(REGISTRY)/$$img:latest; \
		docker push $(REGISTRY)/$$img:latest; \
	done

# ==============================================================================
# Tests
# ==============================================================================

test: test-unit test-integration

# --- Unit tests (Rust) ---
test-unit:
	cd watcher-rs && cargo test -- --nocapture

# --- Claude Code adapter tests (live, requires valid ~/.claude credentials) ---
ADAPTER_TEST_COMPOSE := docker compose -f tests/claude-code-adapter/docker-compose.yaml
test-claude-code-adapter:
	@echo "Building claude-code-adapter..."
	$(ADAPTER_TEST_COMPOSE) build
	@echo "Starting claude-code-adapter..."
	$(ADAPTER_TEST_COMPOSE) up -d
	@echo "Waiting for adapter health..."
	@curl --retry 30 --retry-delay 1 --retry-all-errors -sf http://localhost:18080/health >/dev/null
	@echo "Running tests..."
	@cd tests/claude-code-adapter && pip install -q requests pytest pytest-timeout && \
	pytest . -v --timeout=300 2>&1; \
	EXIT_CODE=$$?; \
	cd ../.. ; \
	echo "Stopping claude-code-adapter..." ; \
	$(ADAPTER_TEST_COMPOSE) down ; \
	exit $$EXIT_CODE

# --- Integration tests ---
# Run the watcher container as the current host user to avoid permission issues
# on bind-mounted directories (sorted/, archive/, etc.).
export PUID ?= $(shell id -u)
export PGID ?= $(shell id -g)

INTEGRATION_COMPOSE := tests/integration/docker-compose.fast.yaml
INTEGRATION_TESTS ?= test_stt.py test_documents.py test_audio.py test_lifecycle.py fixture_tests/

SYNCTHING_COMPOSE := tests/integration/docker-compose.service-mock.yaml
SYNCTHING_TESTS ?= test_stt.py test_migration.py test_documents.py test_audio.py

TESTDATA_DIRS := incoming processed archive reviewed sorted duplicates error unsortable void transit trash .output lost

define integration_clean
	@echo "Generating test data..."
	$(MAKE) -C tests/integration generate
	@echo "Stopping any existing container and removing volumes..."
	docker compose -f $(1) down -v 2>/dev/null || true
	docker compose -f $(1) kill 2>/dev/null || true
	@echo "Cleaning testdata working directories..."
	rm -rf tests/integration/testdata/* 2>/dev/null
endef

define integration_logs
	echo "Saving docker logs..." ; \
	mkdir -p tests/integration/logs ; \
	docker compose -f $(1) logs --no-color > tests/integration/logs/docker.log 2>&1 ; \
	docker cp integration-mrdocument-test-1:/var/log/watcher.log tests/integration/logs/watcher.log 2>/dev/null || true ; \
	docker cp integration-mrdocument-test-1:/var/log/sorter.log tests/integration/logs/sorter.log 2>/dev/null || true ; \
	docker cp integration-mrdocument-test-1:/var/log/mrdocument-service.log tests/integration/logs/service.log 2>/dev/null || true ; \
	docker cp integration-mrdocument-test-1:/var/log/mock-backends.log tests/integration/logs/mock-backends.log 2>/dev/null || true ; \
	echo "Tearing down container and removing volumes..." ; \
	docker compose -f $(1) down -v ; \
	docker compose -f $(1) kill || true
endef

test-integration:
	$(call integration_clean,$(INTEGRATION_COMPOSE))
	@echo "Building integration test containers..."
	docker compose -f $(INTEGRATION_COMPOSE) down -v
	docker compose -f $(INTEGRATION_COMPOSE) kill
	docker compose -f $(INTEGRATION_COMPOSE) build
	@echo "Starting containers..."
	docker compose -f $(INTEGRATION_COMPOSE) up -d --force-recreate
	@echo "Waiting for service to become healthy..."
	@curl --retry 12 --retry-delay 5 --retry-all-errors -sf http://localhost:8000/health > /dev/null
	@echo "Running integration tests..."
	cd tests/integration && bash -c 'set -o pipefail && poetry run pytest $(INTEGRATION_TESTS) -v --timeout=300 2>&1 | tee logs/test-run.log' ; \
	EXIT_CODE=$$? ; \
	cd ../.. ; \
	$(call integration_logs,$(INTEGRATION_COMPOSE)) ; \
	exit $$EXIT_CODE

test-integration-syncthing:
	$(call integration_clean,$(SYNCTHING_COMPOSE))
	@echo "Generating Syncthing configs (if needed)..."
	$(MAKE) -C tests/integration syncthing-config
	@echo "Building integration test containers (with Syncthing)..."
	docker compose -f $(SYNCTHING_COMPOSE) build
	@echo "Starting containers..."
	docker compose -f $(SYNCTHING_COMPOSE) up -d --force-recreate
	@echo "Waiting for service to become healthy..."
	@curl --retry 12 --retry-delay 5 --retry-all-errors -sf http://localhost:8000/health > /dev/null
	@echo "Waiting for Syncthing initial sync..."
	@for i in $$(seq 1 30); do \
		comp=$$(curl -sf -H "X-API-Key: test-api-key-syncthing" \
			"http://localhost:22384/rest/db/completion?folder=mrdocument-testuser" \
			2>/dev/null | python3 -c \
			"import sys,json; print(int(json.load(sys.stdin).get('completion',0)))" \
			2>/dev/null || echo 0); \
		if [ "$$comp" = "100" ]; then echo "Syncthing sync complete."; break; fi; \
		echo "  sync $${comp}% ($$i/30)"; sleep 3; \
	done
	@echo "Running integration tests (with Syncthing)..."
	cd tests/integration && bash -c 'set -o pipefail && poetry run pytest $(SYNCTHING_TESTS) -v --timeout=600 2>&1 | tee logs/test-run.log' ; \
	EXIT_CODE=$$? ; \
	cd ../.. ; \
	$(call integration_logs,$(SYNCTHING_COMPOSE)) ; \
	exit $$EXIT_CODE

# ==============================================================================
# Debug / Peek
# ==============================================================================

peek-watcher:
	$(COMPOSE) exec -it mrdocument-watcher sh

peek-service:
	$(COMPOSE) exec -it mrdocument-service sh

peek-anthropic-adapter:
	$(COMPOSE) exec -it anthropic-adapter sh

peek-stt:
	$(COMPOSE) exec -it stt bash

dump-db:
	$(COMPOSE) up -d mrdocument-db --wait
	docker exec mrdocument-db pg_dump -U mrdocument -d mrdocument --schema=mrdocument --schema-only
	@echo "--- DATA (documents) ---"
	docker exec mrdocument-db psql -U mrdocument -d mrdocument -c "SELECT id, username, status, original_filename, context_name, current_file_path, assigned_filename FROM mrdocument.documents LIMIT 50;"
	@echo "--- DATA (documents_v2) ---"
	docker exec mrdocument-db psql -U mrdocument -d mrdocument -c "SELECT id, state, original_filename, context, source_hash, current_paths FROM mrdocument.documents_v2 LIMIT 50;"
	@echo "--- DATA (file_locations) ---"
	docker exec mrdocument-db psql -U mrdocument -d mrdocument -c "SELECT * FROM mrdocument.file_locations LIMIT 50;" 2>/dev/null || echo "(table does not exist)"

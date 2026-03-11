.PHONY: build up down \
       build-service build-watcher build-stt build-ocrmypdf build-anthropic-adapter build-db \
       push \
       release \
       test test-unit test-integration test-integration-syncthing \
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
REGISTRY   := ghcr.io/mrorganisation
RELEASE_IMAGES := mrdocument-service mrdocument-watcher stt ocrmypdf anthropic-adapter mrdocument-db

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

up: build
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# ==============================================================================
# Push to GHCR
# ==============================================================================

push: build
	@for img in $(RELEASE_IMAGES); do \
		docker tag $$img:latest-custom $(REGISTRY)/$$img:$(IMAGE_TAG); \
		docker tag $$img:latest-custom $(REGISTRY)/$$img:latest-$(BRANCH); \
		docker push $(REGISTRY)/$$img:$(IMAGE_TAG); \
		docker push $(REGISTRY)/$$img:latest-$(BRANCH); \
	done

# ==============================================================================
# Release (tag as X.y.z-branch-commit + latest-branch + latest, then push all)
# ==============================================================================

release: build
	@for img in $(RELEASE_IMAGES); do \
		docker tag $$img:latest-custom $(REGISTRY)/$$img:$(IMAGE_TAG); \
		docker tag $$img:latest-custom $(REGISTRY)/$$img:latest-$(BRANCH); \
		docker tag $$img:latest-custom $(REGISTRY)/$$img:latest; \
		docker push $(REGISTRY)/$$img:$(IMAGE_TAG); \
		docker push $(REGISTRY)/$$img:latest-$(BRANCH); \
		docker push $(REGISTRY)/$$img:latest; \
	done

# ==============================================================================
# Tests
# ==============================================================================

test: test-unit test-integration

# --- Unit tests (Rust) ---
test-unit:
	cd watcher-rs && cargo test -- --nocapture

# --- Integration tests ---
# Run the watcher container as the current host user to avoid permission issues
# on bind-mounted directories (sorted/, archive/, etc.).
export PUID ?= $(shell id -u)
export PGID ?= $(shell id -g)

INTEGRATION_COMPOSE := tests/integration/docker-compose.fast.yaml
INTEGRATION_TESTS ?= test_stt.py test_documents.py test_audio.py test_lifecycle.py

SYNCTHING_COMPOSE := tests/integration/docker-compose.service-mock.yaml
SYNCTHING_TESTS ?= test_stt.py test_migration.py test_documents.py test_audio.py

TESTDATA_DIRS := incoming processed archive reviewed sorted duplicates error unsortable void transit trash .output lost

define integration_clean
	@echo "Generating test data..."
	$(MAKE) -C tests/integration generate
	@echo "Stopping any existing container..."
	docker compose -f $(1) down 2>/dev/null || true
	docker compose -f $(1) kill 2>/dev/null || true
	@echo "Cleaning testdata working directories..."
	@for d in $(TESTDATA_DIRS); do \
		rm -rf tests/integration/testdata/$$d/* 2>/dev/null; \
		mkdir -p tests/integration/testdata/$$d; \
	done
endef

define integration_logs
	echo "Saving docker logs..." ; \
	mkdir -p tests/integration/logs ; \
	docker compose -f $(1) logs --no-color > tests/integration/logs/docker.log 2>&1 ; \
	docker cp integration-mrdocument-test-1:/var/log/watcher.log tests/integration/logs/watcher.log 2>/dev/null || true ; \
	docker cp integration-mrdocument-test-1:/var/log/sorter.log tests/integration/logs/sorter.log 2>/dev/null || true ; \
	docker cp integration-mrdocument-test-1:/var/log/mrdocument-service.log tests/integration/logs/service.log 2>/dev/null || true ; \
	docker cp integration-mrdocument-test-1:/var/log/mock-backends.log tests/integration/logs/mock-backends.log 2>/dev/null || true ; \
	echo "Tearing down container..." ; \
	docker compose -f $(1) down ; \
	docker compose -f $(1) kill || true
endef

test-integration:
	$(call integration_clean,$(INTEGRATION_COMPOSE))
	@echo "Building integration test containers..."
	docker compose -f $(INTEGRATION_COMPOSE) down || true
	docker compose -f $(INTEGRATION_COMPOSE) kill || true
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

.PHONY: build up down \
       build-service build-watcher build-stt build-ocrmypdf build-anthropic-adapter build-db \
       push push-service push-watcher push-stt push-ocrmypdf push-anthropic-adapter push-db \
       test test-unit test-integration test-integration-syncthing test-contexts \
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
COMMIT     := $(shell git rev-parse --short HEAD)
IMAGE_TAG  := $(VERSION)-$(COMMIT)
REGISTRY   := ghcr.io/olekli

# ==============================================================================
# Build / Up / Down
# ==============================================================================

IMAGES := mrdocument-service mrdocument-watcher stt ocrmypdf anthropic-adapter mrdocument-db

build: build-service build-watcher build-stt build-ocrmypdf build-anthropic-adapter build-db

build-service:
	$(COMPOSE) build mrdocument-service --no-cache
	docker tag mrdocument-service:latest-custom $(REGISTRY)/mrdocument-service:$(IMAGE_TAG)

build-watcher:
	$(COMPOSE) build mrdocument-watcher --no-cache
	docker tag mrdocument-watcher:latest-custom $(REGISTRY)/mrdocument-watcher:$(IMAGE_TAG)

build-stt:
	$(COMPOSE) build stt --no-cache
	docker tag stt:latest-custom $(REGISTRY)/stt:$(IMAGE_TAG)

build-ocrmypdf:
	$(COMPOSE) build ocrmypdf
	docker tag ocrmypdf:latest-custom $(REGISTRY)/ocrmypdf:$(IMAGE_TAG)

build-anthropic-adapter:
	$(COMPOSE) build anthropic-adapter
	docker tag anthropic-adapter:latest-custom $(REGISTRY)/anthropic-adapter:$(IMAGE_TAG)

build-db:
	$(COMPOSE) build mrdocument-db
	docker tag mrdocument-db:latest-custom $(REGISTRY)/mrdocument-db:$(IMAGE_TAG)

up: build
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# ==============================================================================
# Push to GHCR
# ==============================================================================

push: push-service push-watcher push-stt push-ocrmypdf push-anthropic-adapter push-db

push-service:
	docker push $(REGISTRY)/mrdocument-service:$(IMAGE_TAG)

push-watcher:
	docker push $(REGISTRY)/mrdocument-watcher:$(IMAGE_TAG)

push-stt:
	docker push $(REGISTRY)/stt:$(IMAGE_TAG)

push-ocrmypdf:
	docker push $(REGISTRY)/ocrmypdf:$(IMAGE_TAG)

push-anthropic-adapter:
	docker push $(REGISTRY)/anthropic-adapter:$(IMAGE_TAG)

push-db:
	docker push $(REGISTRY)/mrdocument-db:$(IMAGE_TAG)

# ==============================================================================
# Tests
# ==============================================================================

test: test-unit test-integration

# --- Unit tests ---
WATCHER_UNIT_TESTS := test_models.py test_prefilter.py test_step1.py test_step2.py \
                      test_step3.py test_step4.py test_step5.py test_step6.py \
                      test_orchestrator_race.py

test-unit:
	cd watcher && python3 -m pytest $(WATCHER_UNIT_TESTS) -v

# --- Integration tests ---
INTEGRATION_COMPOSE := tests/integration/docker-compose.fast.yaml
INTEGRATION_TESTS ?= test_documents.py test_audio.py test_lifecycle.py

SYNCTHING_COMPOSE := tests/integration/docker-compose.service-mock.yaml
SYNCTHING_TESTS ?= test_migration.py test_documents.py test_audio.py

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

test-contexts:
	@echo "Running context-specific tests..."
	$(COMPOSE) up -d mrdocument-service --wait
	cd service && poetry run pytest tests/test_integration.py::TestAiClient::test_determine_context tests/test_integration.py::TestAiClient::test_extract_metadata_with_contexts tests/test_integration.py::TestProcessEndpoint::test_process_with_contexts tests/test_integration.py::TestUtilityFunctions -v

# ==============================================================================
# Debug / Peek
# ==============================================================================

peek-watcher:
	$(COMPOSE) exec -it mrdocument-watcher sh

peek-service:
	$(COMPOSE) exec -it mrdocument-service bash

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

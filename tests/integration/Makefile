.PHONY: install generate generate-documents generate-audio clean mock-up mock-down mock-logs mock-test syncthing-config fast-up fast-down fast-logs fast-test

generate: install generate-documents generate-audio

install:
	poetry install --no-root

generate-documents:
	poetry run python3 generate_documents.py

generate-audio:
	poetry run python3 generate_audio.py

clean:
	rm -f generated/*.pdf generated/*.txt generated/*.rtf generated/*.mp3

mock-up:
	BUILDX_NO_DEFAULT_ATTESTATIONS=1 docker compose -f docker-compose.service-mock.yaml up --build -d

mock-down:
	docker compose -f docker-compose.service-mock.yaml down -v

mock-logs:
	docker compose -f docker-compose.service-mock.yaml logs -f

MOCK_TESTS ?= test_documents.py test_audio.py
FAST_TESTS ?= test_documents.py test_audio.py

mock-test:
	SYNCTHING=1 poetry run pytest $(MOCK_TESTS) -v

fast-up:
	BUILDX_NO_DEFAULT_ATTESTATIONS=1 docker compose -f docker-compose.fast.yaml up --build -d

fast-down:
	docker compose -f docker-compose.fast.yaml down -v

fast-logs:
	docker compose -f docker-compose.fast.yaml logs -f

fast-test:
	poetry run pytest $(FAST_TESTS) -v

syncthing-config:
	@if [ ! -f syncthing/server/cert.pem ]; then \
		bash syncthing/generate_config.sh ; \
	else \
		echo "Syncthing configs exist, skipping." ; \
	fi

# Overview: MrDocument

This is an implementation plan for a webservice that accepts PDF documents, sends them to another service for OCR, then sends the text to an AI
model to extract metadata from it. It forms a filename from the metadata and returns this filename together with the OCR'ed PDF file as
response.

The service for OCR can be found in this project under `ocrmypdf`.

This service shall be implemented in Python using Poetry for management.

# Architecture

There shall be the following modules:

* server.py: Contains the top-level REST serving. It shall be HTTP only. It shall be able to accept multiple connections asynchronously.
  (Each connection, however, is handled synchronously.)
* ocr.py: Python interface to the OCR service.
* ai.py: Python interface to the AI model. For starters, we will implement Anthropic API.

# Tests

There shall be an integration test against mock OCR and AI services.

# Custom Topic Instructions Feature

## Overview

The mrdocument API has been extended to support custom instructions for topic determination. This allows users to provide additional guidance to the AI model on how to determine the `topic` field when processing documents.

## Changes Made

### 1. Configuration (`mrdocument/config.yaml`)
- Added `custom_topic_instruction` template that accepts custom instructions
- Integrated the custom topic instruction into the main prompt

### 2. AI Client (`mrdocument/ai.py`)
- Added `topic_instructions` parameter to `extract_metadata()` method
- Added `topic_instructions` parameter to `_build_prompt()` method
- Loads the custom topic instruction template from config
- Builds and includes custom topic instructions in the AI prompt when provided

### 3. Server API (`mrdocument/server.py`)
- Added `topic_instructions` field to multipart form-data handling
- Passes the custom instructions through to AI client for both PDF and DOCX processing
- Updated logging to include topic_instructions parameter

### 4. Documentation (`README.md`)
- Added `topic_instructions` parameter to API documentation
- Provided usage examples with curl and Python

### 5. Tests (`tests/test_integration.py`)
- Added `test_build_prompt_with_topic_instructions()` to verify prompt building
- Added `test_build_prompt_without_topic_instructions()` to verify default behavior
- Added `test_process_with_topic_instructions()` to verify end-to-end functionality

## Usage

### API Parameter

Add the `topic_instructions` field to your multipart form-data request:

```bash
curl -X POST \
  -F "file=@document.pdf" \
  -F 'topic_instructions=For legal documents, use the case number as the topic. For personal documents, use the subject matter.' \
  http://localhost:8000/process
```

### Python Example

```python
import requests

with open('document.pdf', 'rb') as f:
    response = requests.post(
        'http://localhost:8000/process',
        files={'file': f},
        data={
            'topic_instructions': 'Determine topic based on the project or matter reference in the document header.',
        }
    )

result = response.json()
print(f"Topic: {result['metadata']['topic']}")
```

## How It Works

1. The user provides custom instructions as a string parameter
2. The server receives the instruction and passes it to the AI client
3. The AI client includes the custom instruction in the prompt sent to Claude
4. The AI model considers the custom instruction when determining the topic field
5. The extracted topic is returned in the metadata response

## Backward Compatibility

This feature is fully backward compatible:
- The `topic_instructions` parameter is optional
- If not provided, the AI uses the default topic determination logic
- All existing filters (`types`, `senders`, `topics`) continue to work as before

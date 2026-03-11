"""Mock backends for service-mock integration tests.

Thin re-export wrapper — the actual implementations live in separate modules.
This file exists for backward compatibility with the monolithic supervisord setup.

Usage (unchanged):
    gunicorn --bind 0.0.0.0:9000 --workers 2 --timeout 30 mock_backends:app
    gunicorn --bind 0.0.0.0:8001 --workers 1 --timeout 30 mock_backends:stt_app
"""

import sys
import os

# Ensure the directory containing this file is on sys.path so the
# standalone mock modules can be imported when running via gunicorn.
_dir = os.path.dirname(os.path.abspath(__file__))
if _dir not in sys.path:
    sys.path.insert(0, _dir)

from mock_anthropic import app as _anthropic_app  # noqa: E402
from mock_anthropic import DOCUMENT_METADATA  # noqa: E402, F401
from mock_ocr import app as _ocr_app  # noqa: E402

# The combined app merges OCR + Anthropic endpoints (original port 9000 layout).
# Flask doesn't natively merge apps, so we register the OCR routes onto the
# Anthropic app which already has /v1/messages and /health.
# Override /health to reflect the combined nature.
app = _anthropic_app

# Register OCR endpoint from the OCR app
app.add_url_rule("/ocr", "mock_ocr", _ocr_app.view_functions["mock_ocr"], methods=["POST"])

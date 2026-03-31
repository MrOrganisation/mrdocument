"""Mock OCR backend for integration tests.

Provides a mock OCR service that returns the input PDF bytes back
with dummy extracted text.

Endpoints:
    GET  /health  - Health check
    POST /ocr     - Mock OCR (returns input PDF + dummy text)

Usage:
    gunicorn --bind 0.0.0.0:5000 --workers 2 --timeout 30 mock_ocr:app
"""

import base64

from flask import Flask, jsonify, request

app = Flask("mock_ocr")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "mock-ocr"})


@app.route("/ocr", methods=["POST"])
def mock_ocr():
    """Return input PDF bytes (base64) + dummy text."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    uploaded = request.files["file"]
    file_bytes = uploaded.read()
    filename = uploaded.filename or "document.pdf"

    # Simulate 422 for empty/corrupt documents (non-retryable input error)
    if len(file_bytes) == 0:
        return jsonify({"error": "OCR failed: empty document"}), 422

    # Simulate 422 for encrypted PDFs (content starts with marker)
    if file_bytes[:10] == b"ENCRYPTED:":
        return jsonify({
            "error": "PDF is encrypted and cannot be processed",
            "details": "EncryptedPdfError",
        }), 422

    # Validate PDF magic bytes — real OCR rejects non-PDF input
    if not file_bytes.startswith(b"%PDF"):
        return jsonify({
            "error": (
                f"OCR processing failed. Input file is not a PDF, "
                f"checking if it is an image...\n"
                f"cannot identify image file '/tmp/uploads/{filename}'\n"
                f"UnsupportedImageFormatError\n"
            ),
        }), 500

    pdf_b64 = base64.b64encode(file_bytes).decode("utf-8")
    return jsonify({
        "pdf": pdf_b64,
        "text": f"Mock OCR text for {filename}",
        "filename": f"ocr_{filename}",
    })

FROM python:3.12-slim

# ---------------------------------------------------------------------------
# System packages (union of all service needs)
# ---------------------------------------------------------------------------
# PostgreSQL, OCR, STT, process manager, WeasyPrint deps, RTF conversion
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql postgresql-client \
    tesseract-ocr tesseract-ocr-deu ocrmypdf unpaper poppler-utils \
    ffmpeg \
    supervisor \
    curl \
    libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf-2.0-0 \
    libffi-dev shared-mime-info \
    unrtf \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Python deps — install in stages
# ---------------------------------------------------------------------------

# 1. Watcher dependencies
COPY watcher/requirements.txt /tmp/watcher-requirements.txt
RUN pip install --no-cache-dir -r /tmp/watcher-requirements.txt

# 2. MrDocument service (Poetry)
RUN pip install --no-cache-dir poetry
COPY service/pyproject.toml service/poetry.lock /app/service/
RUN cd /app/service && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-interaction --no-ansi --no-root

# 3. STT service
COPY stt/ /app/stt/
RUN cd /app/stt && pip install --no-cache-dir -e .

# 4. OCR API (Flask + Gunicorn)
RUN pip install --no-cache-dir flask gunicorn

# ---------------------------------------------------------------------------
# Application copies
# ---------------------------------------------------------------------------

# MrDocument service
COPY service/mrdocument /app/service/mrdocument

# Watcher
COPY watcher/sorter.py /app/watcher/sorter.py
COPY watcher/models.py /app/watcher/models.py
COPY watcher/step1.py /app/watcher/step1.py
COPY watcher/step2.py /app/watcher/step2.py
COPY watcher/step3.py /app/watcher/step3.py
COPY watcher/step4.py /app/watcher/step4.py
COPY watcher/step5.py /app/watcher/step5.py
COPY watcher/step6.py /app/watcher/step6.py
COPY watcher/db_new.py /app/watcher/db_new.py
COPY watcher/orchestrator.py /app/watcher/orchestrator.py
COPY watcher/app.py /app/watcher/app.py

# OCR API
COPY ocrmypdf/api.py /app/ocrmypdf/api.py

# Test config files
COPY tests/integration/config/ /app/config/

# Supervisord config
COPY tests/integration/supervisord.conf /etc/supervisord.conf
COPY tests/integration/supervisord.service-mock.conf /etc/supervisord.service-mock.conf

# Mock backends (OCR, AI, STT)
COPY tests/integration/mock_backends.py /app/mock/mock_backends.py
COPY tests/integration/mock_anthropic.py /app/mock/mock_anthropic.py
COPY tests/integration/mock_stt.py /app/mock/mock_stt.py
COPY tests/integration/mock_ocr.py /app/mock/mock_ocr.py

# Entrypoint
COPY tests/integration/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# ---------------------------------------------------------------------------
# Create directories
# ---------------------------------------------------------------------------
RUN mkdir -p /data /var/log/supervisor /tmp/uploads /tmp/outputs

# ---------------------------------------------------------------------------
# Environment defaults (wired internally)
# ---------------------------------------------------------------------------
ENV MRDOCUMENT_URL=http://localhost:8000 \
    STT_URL=http://localhost:8001 \
    OCR_URL=http://localhost:5000 \
    DATABASE_URL=postgresql://mrdocument:mrdocument@localhost:5432/mrdocument \
    OCR_LANGUAGE=deu+eng \
    WATCHER_CONFIG=/app/watcher.yaml \
    HEALTH_PORT=8080 \
    HOST=0.0.0.0 \
    PORT=8000

# Runtime-required: ANTHROPIC_API_KEY
# Optional: ELEVENLABS_API_KEY

EXPOSE 8000 8080

ENTRYPOINT ["/app/entrypoint.sh"]

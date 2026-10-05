# One image, three commands: the ms-ingest and ms-backfill Cloud Run jobs override the command
# (python -m stibms.ingest ...); the ms-api service will run uvicorn on $PORT (added with the API).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Nightly default: ingest yesterday's service day into $MS_BUCKET.
CMD ["python", "-m", "stibms.ingest", "--date", "yesterday"]

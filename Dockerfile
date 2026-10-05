# syntax=docker/dockerfile:1.7
# One image, three commands: the ms-ingest and ms-backfill Cloud Run jobs override the command
# (python -m stibms.ingest ...); the ms-api service runs uvicorn on $PORT (default CMD below).
#
# The microsegments package comes from the `microsegments-src` stage. By default that stage is empty
# and the package is installed from $MICROSEGMENTS (PyPI). For a local build against a
# working copy, override the stage with a build context (scripts/docker-build.sh does this):
#   docker build --build-context microsegments-src=/path/to/clean/copy -t stibms .
FROM scratch AS microsegments-src

FROM python:3.12-slim
ARG MICROSEGMENTS="microsegments>=0.2.0,<0.3"
ARG MS_VERSION=0.0.0
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    MS_CACHE=/tmp/stibms-cache PORT=8080
WORKDIR /app

COPY --from=microsegments-src / /tmp/microsegments/
RUN if [ -f /tmp/microsegments/pyproject.toml ]; then \
        SETUPTOOLS_SCM_PRETEND_VERSION="${MS_VERSION}" pip install /tmp/microsegments; \
    else \
        apt-get update && apt-get install -y --no-install-recommends git && \
        pip install "$MICROSEGMENTS" && \
        apt-get purge -y git && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*; \
    fi && rm -rf /tmp/microsegments

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install '.[api]'

# API by default; the jobs override the command with python -m stibms.ingest ...
CMD ["sh", "-c", "exec uvicorn stibms.api:app --host 0.0.0.0 --port ${PORT:-8080}"]

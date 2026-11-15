# One image for every AccessSentinel Python service; each compose service picks its command.

# Stage 1: build the React + TypeScript dashboard (served by the `api` service).
FROM node:20.18.1-alpine AS dashboard
WORKDIR /build
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

# Stage 2: Python services.
FROM python:3.11.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# CPU-only PyTorch from the official wheel index (much smaller than the default CUDA build).
RUN pip install --index-url https://download.pytorch.org/whl/cpu "torch==2.15.0"

COPY requirements/ requirements/
RUN pip install -r requirements/services.txt

COPY accesssentinel/ accesssentinel/
COPY scripts/ scripts/
COPY --from=dashboard /build/dist/ /app/dashboard_dist/

# Spool for events Kafka could not accept; created here so a named volume inherits the runtime user's ownership.
RUN mkdir -p /app/accesssentinel_data/spool

# Unprivileged runtime user.
RUN useradd --uid 10001 --create-home app && chown -R app:app /app
USER app

CMD ["python", "-c", "import accesssentinel, sys; print('accesssentinel', accesssentinel.__version__); sys.exit(0)"]

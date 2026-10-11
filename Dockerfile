# Multi-stage Python 3.12 Slim Container
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final production runtime stage
FROM python:3.12-slim AS runner

WORKDIR /app

# Create unprivileged service user
RUN groupadd -r agentsquad && useradd -r -g agentsquad -d /app -s /sbin/nologin agentsquad

# Copy installed python dependencies from builder
COPY --from=builder /root/.local /home/agentsquad/.local
ENV PATH=/home/agentsquad/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV PORT=8500
ENV HOST=0.0.0.0

# Copy application source
COPY --chown=agentsquad:agentsquad . .

# Ensure data directory permissions
RUN mkdir -p /app/data && chown -R agentsquad:agentsquad /app/data

USER agentsquad

EXPOSE 8500

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8500/healthz')" || exit 1

CMD ["python", "main.py"]

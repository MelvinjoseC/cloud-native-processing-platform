# Stage 1: Build dependencies
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Final minimal image
FROM python:3.12-slim AS runner

# Metadata labels
LABEL org.opencontainers.image.title="Cloud-Native Job Worker" \
      org.opencontainers.image.description="Asynchronous processing worker scaling with KEDA" \
      org.opencontainers.image.source="https://github.com/MelvinjoseC/cloud-native-processing-platform" \
      org.opencontainers.image.version="1.0.0"

WORKDIR /app

# Create a non-root user and group
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/bash appuser

# Copy installed python dependencies from builder
COPY --from=builder --chown=appuser:appgroup /root/.local /home/appuser/.local
COPY --chown=appuser:appgroup main.py .

# Update PATH environment variable
ENV PATH=/home/appuser/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

USER 1000:1000

EXPOSE 9102

CMD ["python", "main.py"]

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY tdgen_temporal/ tdgen_temporal/
COPY config/ config/
COPY ui/ ui/

# Output volume for generated data
VOLUME ["/app/output", "/app/logs"]

# Default: show help. Override CMD to run specific commands.
# docker run --rm -v $(pwd)/output:/app/output tdgen-temporal init --date 2024-01-01
# docker run --rm -v $(pwd)/output:/app/output tdgen-temporal advance --days 7
ENTRYPOINT ["python", "-m", "tdgen_temporal.cli"]
CMD ["--help"]

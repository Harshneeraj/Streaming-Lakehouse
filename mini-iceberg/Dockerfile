FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
      "pyarrow==17.0.0" \
      "fsspec==2024.6.1" \
      "s3fs==2024.6.1" \
      "duckdb==1.0.0" \
      "rich==13.7.1"

# Library + examples are bind-mounted at runtime for fast iteration
ENV PYTHONPATH=/app
CMD ["python", "-c", "print('mini-iceberg image ready')"]

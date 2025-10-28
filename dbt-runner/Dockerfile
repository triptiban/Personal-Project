# dbt-runner/Dockerfile  (build from repo root)
FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY dbt-runner/requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Copy your dbt project into the image
# (Path is relative to build context = repo root)
COPY ecommerce_dbt /work/ecommerce_dbt

# Place profiles.yml that uses env_var() for connection details
RUN mkdir -p /work/ecommerce_dbt/.dbt
COPY dbt-runner/profiles.yml /work/ecommerce_dbt/.dbt/profiles.yml

WORKDIR /work/ecommerce_dbt

# Use JSON form of CMD to handle signals properly
#CMD ["bash", "-lc", "dbt run && dbt test && dbt docs generate"]
CMD ["dbt", "--help"]

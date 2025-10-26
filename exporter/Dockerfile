FROM python:3.12-slim
WORKDIR /app
COPY export_curated.py /app/
RUN pip install --no-cache-dir pandas psycopg2-binary minio
CMD ["python", "export_curated.py"]

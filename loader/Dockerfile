FROM python:3.12-slim
RUN pip install --no-cache-dir minio==7.2.7 psycopg2-binary==2.9.9
WORKDIR /app
COPY load.py /app/load.py
CMD ["python", "/app/load.py"]

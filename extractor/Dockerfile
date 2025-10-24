FROM python:3.12-slim
RUN pip install --no-cache-dir requests==2.32.3 minio==7.2.7
WORKDIR /app
COPY app.py /app/app.py
CMD ["python", "/app/app.py"]


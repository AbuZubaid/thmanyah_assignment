FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY redis-sink.py .
CMD ["python", "redis-sink.py"]
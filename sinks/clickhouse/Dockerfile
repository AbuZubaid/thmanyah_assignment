FROM python:3.11-slim
RUN pip install kafka-python clickhouse-connect
WORKDIR /app
COPY ch_sink.py /app/
CMD ["python", "ch_sink.py"]

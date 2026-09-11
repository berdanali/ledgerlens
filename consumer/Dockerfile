FROM python:3.12-slim

WORKDIR /app

# gcc is needed only if confluent-kafka falls back to source build on this arch;
# for standard amd64/arm64 the manylinux wheel ships librdkafka bundled.
RUN apt-get update && apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

COPY consumer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/ consumer/

CMD ["python", "-m", "consumer.main"]

FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
      "confluent-kafka==2.5.0" \
      "Faker==26.0.0" \
      "orjson==3.10.7"

# Producer source is bind-mounted at runtime via docker-compose; the image
# only carries the deps so iteration is fast.

FROM python:3.11-slim

WORKDIR /app

# confluent-kafka ships manylinux wheels, so there is no build toolchain here.
COPY pyproject.toml README.md ./
COPY telemetry ./telemetry
RUN pip install --no-cache-dir -e .

COPY services ./services
COPY scripts ./scripts
COPY prometheus/label-budgets.json ./prometheus/label-budgets.json

ENV PYTHONUNBUFFERED=1
CMD ["python", "services/checkout_api.py"]

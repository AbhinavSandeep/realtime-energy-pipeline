FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY events.py kafka_utils.py producer.py inject_event.py ./

# Run as an unprivileged user (security: containers share the host kernel)
RUN useradd --create-home --uid 10001 app \
 && mkdir -p /state /data \
 && chown app:app /state /data
USER app

CMD ["python", "producer.py"]

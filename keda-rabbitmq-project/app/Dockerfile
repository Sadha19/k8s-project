
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    pika==1.3.2 \
    prometheus-client==0.23.1 \
    && useradd --system --uid 10001 worker

COPY --chown=worker:worker worker.py /app/worker.py

USER 10001:10001

EXPOSE 8000

CMD ["python", "-u", "/app/worker.py"]

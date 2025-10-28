# syntax=docker/dockerfile:1
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY . /app

RUN python -m pip install --upgrade pip

EXPOSE 8080

ENTRYPOINT ["python", "-m", "procplan.server"]
CMD ["--config", "/data/config.json", "--database", "/data/procplan.db", "--host", "0.0.0.0", "--port", "8080"]

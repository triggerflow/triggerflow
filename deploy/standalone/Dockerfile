FROM python:3.8.2-slim

RUN apt-get update && apt-get upgrade -y \
        && rm -rf /var/lib/apt/lists/*

COPY deploy/requirements.txt requirements.txt

RUN pip install --upgrade eventlet gunicorn pip setuptools kubernetes six \
    && pip install --no-cache-dir -r requirements.txt

ENV PORT 5000
ENV PYTHONUNBUFFERED TRUE

EXPOSE 5000

WORKDIR /controller

COPY triggerflow ./triggerflow

COPY deploy/standalone/standalone_config_map.yaml config_map.yaml
COPY deploy/standalone/controller.py .

CMD python controller.py

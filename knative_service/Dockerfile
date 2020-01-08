FROM python:3.6

RUN apt-get update && apt-get upgrade -y

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip setuptools six && pip install --no-cache-dir -r requirements.txt

ENV PORT 8080

RUN mkdir -p /event-processor
ADD *.py /event-processor/
ADD actions /event-processor/actions
ADD brokers /event-processor/brokers
ADD conditions /event-processor/conditions
ADD libs /event-processor/libs
ADD config.yaml /event-processor

# Automatically curl the health endpoint every 5 minutes.
# If the endpoint doesn't respond within 30 seconds, kill the main python process.
# As of docker 1.12, a failed healthcheck never results in the container being
# restarted. Killing the main process is a way to make the restart policy kicks in.
#HEALTHCHECK --interval=5m --timeout=1m CMD curl -m 30 --fail http://localhost:5000/health || killall python

CMD ["/bin/bash", "-c", "cd event-processor && python -u app.py"]

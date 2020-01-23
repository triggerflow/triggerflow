FROM python:3.6

RUN apt-get update && apt-get upgrade -y

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip setuptools six && pip install --no-cache-dir -r requirements.txt

ENV PORT 8080

RUN mkdir -p /event-processor
ADD *.py /event-processor/
ADD actions /event-processor/actions
ADD event_source_hooks /event-processor/event_source_hooks
ADD conditions /event-processor/conditions
ADD libs /event-processor/libs
ADD config.yaml /event-processor

CMD ["/bin/bash", "-c", "cd event-processor && python -u app.py"]

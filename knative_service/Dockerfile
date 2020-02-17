FROM python:3.6

RUN apt-get update && apt-get upgrade -y

COPY knative_service/requirements.txt requirements.txt

RUN pip install --upgrade pip setuptools six && pip install --no-cache-dir -r requirements.txt

ENV PORT 8080

ENV APP_HOME /event-processor
WORKDIR $APP_HOME

ADD knative_service/*.py ./
ADD standalone_service .
ADD config.yaml .

CMD exec gunicorn --bind :$PORT --workers 1 appproxy:proxy
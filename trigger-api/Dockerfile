FROM python:3.8.2-slim

COPY trigger-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PORT 8080
ENV APP_HOME /triggerflow-api

WORKDIR $APP_HOME

COPY trigger-api/api ./api

COPY trigger-api/setup.py .

COPY triggerflow/ api/triggerflow

CMD python3 setup.py && python3 api/api.py
FROM python:3.8.2-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PORT 8080
ENV APP_HOME /api

WORKDIR $APP_HOME
COPY ["./api", "setup.py", "./"]

CMD python3 setup.py && python3 api.py
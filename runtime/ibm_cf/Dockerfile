# Based on: https://github.com/ibm-functions/runtime-python/tree/master/python3.6

FROM ibmfunctions/action-python-v3.6

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip setuptools six && pip install --no-cache-dir -r requirements.txt

COPY actionproxy.py /actionProxy/actionproxy.py

COPY pythonrunner.py /pythonAction/pythonrunner.py

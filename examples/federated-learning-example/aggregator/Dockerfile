FROM ibmfunctions/action-python-v3.6

RUN apt-get update
RUN pip3 install -U setuptools

COPY triggerflow /triggerflow
RUN cd triggerflow && \
    python3 setup.py install && \
    cd ..

COPY cloudbutton /cloudbutton
RUN cd cloudbutton && \
    python3 setup.py develop

RUN pip3 install -U redis
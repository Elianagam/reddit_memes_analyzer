FROM python:3.9.7-alpine
RUN apk update
RUN apk add docker
RUN pip3 install --user pika
RUN pip3 install --user retry
COPY health_check /src
COPY common /src/common

WORKDIR /src
CMD python3 -u main.py

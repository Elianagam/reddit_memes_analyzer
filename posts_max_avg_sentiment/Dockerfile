FROM python:3.9.7-alpine
RUN apk update
RUN apk add docker
RUN pip3 install --user pika
RUN pip3 install --user retry
RUN pip3 install --user requests
RUN pip3 install --user wget

COPY posts_max_avg_sentiment /
COPY common /common
CMD ["python", "./main.py"]

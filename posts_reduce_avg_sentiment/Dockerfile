FROM python:3.9.7-alpine
RUN apk update
RUN apk add docker
RUN pip3 install --user pika
RUN pip3 install --user retry

COPY posts_reduce_avg_sentiment /
COPY common /common
CMD ["python", "./main.py"]

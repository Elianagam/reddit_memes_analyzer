FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY posts_reduce_avg_sentiment /
COPY common /common
CMD ["python", "./main.py"]
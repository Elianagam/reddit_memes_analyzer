FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika requests wget

COPY posts_max_avg_sentiment /
COPY common /common
CMD ["python", "./main.py"]
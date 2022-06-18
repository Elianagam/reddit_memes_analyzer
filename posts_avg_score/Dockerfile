FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY posts_avg_score /
COPY common /common
CMD ["python", "./main.py"]
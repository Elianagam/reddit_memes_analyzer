FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika
RUN pip3 install atomicwrites

COPY join_comments_with_posts /
COPY common /common
CMD ["python", "./main.py"]
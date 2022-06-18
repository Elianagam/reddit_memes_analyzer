FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY client /
COPY data /data
COPY common /common
CMD ["python", "./main.py"]
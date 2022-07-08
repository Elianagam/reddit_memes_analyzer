FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika
RUN pip install --user retry
RUN pip3 install --user atomicwrites

COPY receiver /
COPY common /common
CMD ["python", "./main.py"]

FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika


ENV HOST=rabbitmq
ENV PORT=5672

ENV FILE_COMMETS=data/comments.py
ENV FILE_POSTS=data/post.csv
ENV CHUNKSIZE=400
ENV POSTS_QUEUE=client_posts_queue
ENV COMMETS_QUEUE=client_comments_queue
ENV STUDENTS_QUEUE=student_url_queue
ENV AVG_QUEUE=posts_avg_score_queue
ENV IMAGE_QUEUE=post_avg_sentiments_queue

COPY client /
COPY data /data
COPY common/ /common
ENTRYPOINT ["python", "./main.py"]

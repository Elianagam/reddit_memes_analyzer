import logging
import signal
import csv
import json
import sys
from multiprocessing import Process
from common.connection import Connection


class Client:
    def __init__(
        self,
        file_posts,
        posts_queue,
        file_comments,
        comments_queue,
        chunksize,
        students_queue,
        avg_queue,
        image_queue
    ):
        logging.info("INIT")
        self.file_comments = file_comments
        self.file_posts = file_posts
        self.chunksize = chunksize

        self.conn_posts = Connection(queue_name=posts_queue)
        self.conn_comments = Connection(queue_name=comments_queue, conn=self.conn_posts)

        self.students_recved = []
        self.conn_recv_students = Connection(queue_name=students_queue)
        self.conn_recv_avg = Connection(exchange_name=avg_queue, bind=True, conn=self.conn_recv_students)
        self.conn_recv_image = Connection(queue_name=image_queue, conn=self.conn_recv_students)

        self.comments_sender = Process(target=self.__send_comments())
        self.posts_sender = Process(target=self.__send_posts())
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_posts.close()
        self.conn_comments.close()
        self.conn_recv_students.close()
        sys.exit(0)


    def start(self):
        self.posts_sender.start()
        self.comments_sender.start()

        self.__recv_sinks()

        self.comments_sender.join()
        self.posts_sender.join()
    

    def __send_posts(self):
        logging.info("SEND POST DATA")
        fields = ["type", "id", "subreddit.id", "subreddit.name", 
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "domain", "url", "selftext", "title", "score"]

        self.__read(self.file_posts, self.conn_posts, fields)

    def __read(self, file_name, conn, fields):
        with open(file_name, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            chunk = []
            for i, line in enumerate(reader):
                if (i % self.chunksize == 0 and i > 0):
                    logging.info(f"CHUNK {len(chunk)}")
                    conn.send(body=json.dumps(chunk))
                    chunk = []
                chunk.append(line)
            
            if len(chunk) != 0:
                conn.send(body=json.dumps(chunk))

            logging.info(f"CHUNK {file_name} - {len(chunk)}")
            conn.send(body=json.dumps({"end": True}))

    def __send_comments(self):
        logging.info("SEND COMMENTS DATA")
        fields = ["type","id", "subreddit.id", "subreddit.name",
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "body", "sentiment", "score"]

        self.__read(self.file_comments, self.conn_comments, fields)

    def __recv_sinks(self):
        self.conn_recv_students.recv(self.__callback_students, start_consuming=False)
        self.conn_recv_avg.recv(self.__callback, start_consuming=False)
        self.conn_recv_image.recv(self.__callback)
        

    def __callback_students(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        
        if "end" in sink_recv:
            return
        for student in sink_recv:
            logging.info(f"* * * [CLIENT RECV END STUDENT] {sink_recv}")
            self.students_recved.append(student)
        

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        if "end" in sink_recv:
            return
        else:
            if "posts_score_avg" in sink_recv:
                logging.info(f"* * * [CLIENT RECV] {sink_recv}")
            else:
                logging.info(f"* * * [CLIENT RECV] {sink_recv.keys()}")

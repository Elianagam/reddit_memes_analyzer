import logging
import csv
import json
import signal
import sys

from multiprocessing import Process
from common.connection import Connection


class ResponseHandler(Process):
    def __init__(self, response_queue):
        super(ResponseHandler, self).__init__()
        self.conn_recv_response = Connection(queue_name=response_queue)
        #signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv_response.close()
        #sys.exit(0)

    def start(self):
        self.conn_status_recv.recv(self.__callback_status)

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)

        if "posts_score_avg" in sink_recv:
            logging.info(f"* * * [CLIENT AVG_SCORE RECV] {sink_recv}")
        elif "image_bytes" in sink_recv:
            logging.info(f"* * * [CLIENT BYTES RECV] {sink_recv.keys()}")
        else: 
            logging.info(f"* * * [CLIENT STUDENT RECV] {len(sink_recv)}")



class DataSender(Process):

    def __init__(self, file_posts, file_comments, posts_queue, comments_queue):
        super(DataSender, self).__init__()
        self.file_comments = file_comments
        self.file_posts = file_posts

        self.conn_posts = Connection(queue_name=posts_queue)
        self.conn_comments = Connection(queue_name=comments_queue, conn=self.conn_posts)
        logging.info("INIT SEND")
        self.posts = Process(target=self.__send_posts())
        self.comments = Process(target=self.__send_comments())

        #signal.signal(signal.SIGTERM, self.exit_gracefully)

    def start(self):
        self.post.start()
        self.comments.start()
        self.posts.join()
        self.comments.join()

    def exit_gracefully(self, *args):
        self.conn_posts.close()
        #sys.exit(0)

    def __send_posts(self):
        logging.info("SEND POST DATA")
        fields = ["type", "id", "subreddit.id", "subreddit.name", 
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "domain", "url", "selftext", "title", "score"]

        self.__read(self.file_posts, self.conn_posts, fields)

    def __send_comments(self):
        logging.info("SEND COMMENTS DATA")
        fields = ["type","id", "subreddit.id", "subreddit.name",
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "body", "sentiment", "score"]

        self.__read(self.file_comments, self.conn_comments, fields)

    def __read(self, file_name, conn, fields):
        with open(file_name, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            chunk = []
            for i, line in enumerate(reader):
                if (i % self.chuconn_postsnksize == 0 and i > 0):
                    logging.info(f"CHUNK {len(chunk)}")
                    conn.send(body=json.dumps(chunk))
                    chunk = []
                chunk.append(line)
            
            if len(chunk) != 0:
                conn.send(body=json.dumps(chunk))

            logging.info(f"CHUNK {file_name} - {len(chunk)}")
            conn.send(body=json.dumps({"end": True}))
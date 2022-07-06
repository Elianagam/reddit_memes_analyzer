import logging
import csv
import json
import signal
import sys
import time

from multiprocessing import Process
from common.connection import Connection


class StatusChecker(Process):
    def __init__(self, alive, conn_status_send, client_id):
        super(StatusChecker, self).__init__()
        self.alive = alive
        self.client_id = client_id
        self.conn_status_send = conn_status_send

    def exit_gracefully(self, *args):
        logging.info("close status checker...")
        self.conn_status_send.close()
        sys.exit(0)


    def start(self):
        logging.info(f"IS ALIVE? {self.alive.value}")
        while self.alive.value == True:
            logging.info(f"--- [SEND STATUS CHECK]")
            self.conn_status_send.send(body=json.dumps({"client_id": self.client_id}))
            time.sleep(2)


class DataSender(Process):

    def __init__(self, file_posts, file_comments, posts_queue, comments_queue, chunksize):
        super(DataSender, self).__init__()
        self.file_comments = file_comments
        self.file_posts = file_posts
        self.chunksize = chunksize

        self.conn_posts = Connection(queue_name=posts_queue, timeout=1)
        self.conn_comments = Connection(queue_name=comments_queue, conn=self.conn_posts)

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def start(self):
        self.__send_posts()
        self.__send_comments()

    def exit_gracefully(self, *args):
        self.conn_posts.close()
        sys.exit(0)

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
                if (i % self.chunksize == 0 and i > 0):
                    time.sleep(1)
                    logging.info(f"CHUNK {len(chunk)}")
                    conn.send(body=json.dumps(chunk))
                    chunk = []
                chunk.append(line)
            
            if len(chunk) != 0:
                conn.send(body=json.dumps(chunk))

            logging.info(f"CHUNK {file_name} - {len(chunk)}")
            conn.send(body=json.dumps({"end": True}))
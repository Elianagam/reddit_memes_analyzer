import logging
import signal
import csv
import json
import sys
import time
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
        response_queue,
        status_check_queue,
        status_response_queue,
    ):
        logging.info("INIT")
        self.file_comments = file_comments
        self.file_posts = file_posts
        self.chunksize = chunksize
        self.finish = False
        self.run = False

        self.conn_posts = Connection(queue_name=posts_queue)
        self.conn_comments = Connection(queue_name=comments_queue, conn=self.conn_posts)

        self.conn_recv_response = Connection(queue_name=response_queue)
        self.conn_status_send = Connection(queue_name=status_check_queue, conn=self.conn_recv_response)
        #self.conn_status_recv = Connection(queue_name=status_response_queue, conn=self.conn_recv_response)

        self.comments_sender = Process(target=self.__send_comments())
        self.posts_sender = Process(target=self.__send_posts())
        self.check_status = Process(target=self.__status_checker())

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_posts.close()
        self.conn_recv_response.close()
        sys.exit(0)

    def start(self):
        #self.conn_status_recv.recv(self.__callback_status, start_consuming=False)
        self.conn_recv_response.recv(self.__callback)

        #self.check_status.start()
        self.posts_sender.start()
        self.comments_sender.start()
        
        #self.check_status.join()
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
                    #logging.info(f"CHUNK {len(chunk)}")
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

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)

        if "posts_score_avg" in sink_recv:
            logging.info(f"* * * [CLIENT AVG_SCORE RECV] {sink_recv}")
        elif "image_bytes" in sink_recv:
            logging.info(f"* * * [CLIENT BYTES RECV] {sink_recv.keys()}")
        elif "status" in sink_recv:
            self.__callback_status(ch, method, properties, body)
        else: 
            logging.info(f"* * * [CLIENT STUDENT RECV] {len(sink_recv)}")


    def __status_checker(self):
        while not self.finish:
            logging.info(f"--- [SEND STATUS CHECK]")
            self.conn_status_send.send(body=json.dumps({"client_id": 1}))
            time.sleep(5)
            self.conn_recv_response.recv(self.__callback)

    def __callback_status(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        logging.info(f"--- [CLIENT STATUS] {sink_recv}")
        if sink_recv["status"] == "FINISH":
            logging.info(f"[CLOSE CLIENT]")
            self.finish = True
            self.exit_gracefully()
        elif sink_recv["status"] == "BUSY":
            logging.info("System is busy, try later...")
            self.exit_gracefully()
        elif sink_recv["status"] == "AVAILABLE":
            self.run = True
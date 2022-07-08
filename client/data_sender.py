import logging
import csv
import json
import signal
import sys
import time
import os

from multiprocessing import Process
from common.connection import Connection
from common.utils import logger, chunked
from common.health_check.utils.signals import register_handler, SigTermException


class StatusChecker:
    def __init__(self, alive, conn_status_send, client_id):
        super(StatusChecker, self).__init__()
        self.alive = alive
        self.client_id = client_id
        self.conn_status_send = conn_status_send
        self.process = None
        register_handler()

    def start(self):
        logger.info("Starting Status Checker process")
        process = Process(target=self._run)
        process.daemon = True
        process.start()
        self.process = process

    def _run(self):
        try:
            while self.alive.value:
                logger.info(f"--- [SEND STATUS CHECK]")
                self.conn_status_send.send(body=json.dumps({"client_id": self.client_id}))
                logger.info("[%d] Status Checker", os.getpid())
                time.sleep(30)
        except SigTermException:
            logger.info("Received Sigterm")

    def terminate(self, *args):
        if self.process:
            self.process.terminate()
            self.process.join()
            self.process = None

        self.conn_status_send.close()
        sys.exit(0)


class DataSender:

    def __init__(self, file_posts, file_comments, posts_queue, comments_queue, chunksize):
        super(DataSender, self).__init__()
        self.file_comments = file_comments
        self.file_posts = file_posts
        self.chunksize = chunksize

        self.conn_posts = Connection(queue_name=posts_queue, timeout=1)
        self.conn_comments = Connection(queue_name=comments_queue, conn=self.conn_posts)
        self.process = None

        register_handler()

    def start(self):
        logger.info("Starting Data Sender process")
        process = Process(target=self._run)
        process.daemon = True
        process.start()
        self.process = process

    def _run(self):
        try:
            self.__send_posts()
            self.__send_comments()
        except SigTermException:
            logger.info("Received Sigterm")

    def terminate(self, *args):
        if self.process:
            self.process.terminate()
            self.process.join()
            self.process = None

        self.conn_posts.close()
        sys.exit(0)

    def __send_posts(self):
        logger.info("SEND POST DATA")
        fields = ["type", "id", "subreddit.id", "subreddit.name", 
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "domain", "url", "selftext", "title", "score"]

        self.__read(self.file_posts, self.conn_posts, fields)

    def __send_comments(self):
        logger.info("SEND COMMENTS DATA")
        fields = ["type","id", "subreddit.id", "subreddit.name",
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "body", "sentiment", "score"]

        self.__read(self.file_comments, self.conn_comments, fields)

    def __read(self, file_name, conn, fields):
        with open(file_name, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)

            for chunk in chunked(iterable=reader, n=self.chunksize):
                conn.send(body=json.dumps(chunk))

            logger.info(f"CHUNK {file_name} - {len(chunk)}")
            conn.send(body=json.dumps({"end": True}))

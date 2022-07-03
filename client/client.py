import logging
import signal
import csv
import json
import sys
import time
from multiprocessing import Process, Manager
from common.connection import Connection
from data_sender import DataSender, StatusChecker


TIMEOUT = 3


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
        manager = Manager()
        self.alive = manager.Value('alive', True)

        self.file_posts = file_posts
        self.file_comments = file_comments
        self.posts_queue = posts_queue
        self.comments_queue = comments_queue
        self.chunksize = chunksize
        self.response_queue = response_queue
        self.checker = None
        self.data_sender = None

        self.conn_recv_response = Connection(queue_name=response_queue, timeout=1)
        self.conn_status_send = Connection(queue_name=status_check_queue, timeout=1)

        self.channel = self.conn_recv_response.get_channel()
        
        logging.info("INIT")
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv_response.close()
        logging.info(f"join")
        if self.checker != None and self.data_sender != None:
            self.checker.join()
            self.data_sender.join()
        sys.exit(0)

    def start(self):
        logging.info("send status checker...")

        self.conn_status_send.send(body=json.dumps({"client_id": 1}))
        logging.info("waiting status response...")
        status = self.get_status()
        
        logging.info(f"STATUS: {status}")
        
        if status == "AVAILABLE":
            self.data_sender = DataSender(self.file_posts, self.file_comments, 
                self.posts_queue, self.comments_queue, self.chunksize).start()
            self.checker = StatusChecker(self.alive, self.conn_status_send).start()

            logging.info(f"response")
            self.get_response(self.__callback)
            
            
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

    def __callback_status(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        logging.info(f"--- [CLIENT STATUS] {sink_recv}")
        if sink_recv["status"] == "FINISH":
            logging.info(f"[CLOSE CLIENT]")
            self.alive.value = False
            self.exit_gracefully()
        elif sink_recv["status"] == "BUSY":
            logging.info("System is busy, try later...")
            self.exit_gracefully()
        elif sink_recv["status"] == "AVAILABLE":
            self.alive.value = True
        elif sink_recv["status"] == "PENDING":
            self.alive.value = True

    def get_status(self):
        for method, properties, body in self.channel.consume(self.response_queue, inactivity_timeout=TIMEOUT):
            if body != None:
                msg = json.loads(body)
                if "status" in msg:
                    return msg["status"]
                self.channel.basic_ack(method.delivery_tag)

    def get_response(self, callback):
        for method, properties, body in self.channel.consume(self.response_queue, inactivity_timeout=TIMEOUT):
            if body != None:
                callback(self.channel,method, properties, body)
                self.channel.basic_ack(method.delivery_tag)
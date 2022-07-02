import logging
import signal
import csv
import json
import sys
import time
from multiprocessing import Process
from common.connection import Connection
from data_sender import DataSender

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
        self.run = True

        self.conn_recv_response = Connection(queue_name=response_queue)
        self.conn_status_send = Connection(queue_name=status_check_queue)
        
        logging.info("INIT")
        self.data_sender = DataSender(file_posts, file_comments, posts_queue, comments_queue, chunksize)
        self.data_sender.start()
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv_response.close()
        sys.exit(0)

    def start(self):
        self.conn_recv_response.recv(self.__callback)
        
        self.data_sender.join()
        #self.conn_status_send.send(body=json.dumps({"client_id": 1}))

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
        while self.run:
            logging.info(f"--- [SEND STATUS CHECK]")
            self.conn_status_send.send(body=json.dumps({"client_id": 1}))
            time.sleep(5)
            self.conn_recv_response.recv(self.__callback)

    def __callback_status(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        logging.info(f"--- [CLIENT STATUS] {sink_recv}")
        if sink_recv["status"] == "FINISH":
            logging.info(f"[CLOSE CLIENT]")
            self.run = False
            self.exit_gracefully()
        elif sink_recv["status"] == "BUSY":
            logging.info("System is busy, try later...")
            self.exit_gracefully()
        elif sink_recv["status"] == "AVAILABLE":
            self.run = True
        return

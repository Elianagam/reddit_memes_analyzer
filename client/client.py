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
        client_id
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
        self.client_id = client_id
        self.data_to_recv = 0
        self.data_recved = 0
        self.worker_key_status = f"status.client{self.client_id}"
        self.worker_key_response = f"response.client{self.client_id}"

        self.conn_recv_response = Connection(exchange_name=response_queue, bind=True,
            exchange_type='topic', routing_key=self.worker_key_response, timeout=1)

        self.conn_recv_status = Connection(exchange_name=response_queue, bind=True,
            exchange_type='topic', routing_key=self.worker_key_status, timeout=1)

        self.conn_status_send = Connection(queue_name=status_check_queue, timeout=1)

        self.channel_response = self.conn_recv_response.get_channel()        
        self.channel_status = self.conn_recv_status.get_channel()        
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv_response.close()
        if self.checker != None and self.data_sender != None:
            self.checker.join()
            self.data_sender.join()
        sys.exit(0)

    def start(self):
        self.conn_status_send.send(body=json.dumps({"client_id": self.client_id}))
        logging.info("waiting status response...")
        status = self.get_status()

        logging.info(f"STATUS: {status}")
        
        if status["status"] == "AVAILABLE":
            self.data_sender = DataSender(self.file_posts, self.file_comments, 
                self.posts_queue, self.comments_queue, self.chunksize).start()

            self.checker = StatusChecker(self.alive, self.conn_status_send, self.client_id).start()
            self.get_response(self.__callback)
            
            
    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)

        if "posts_score_avg" in sink_recv:
            logging.info(f"* * * [AVG_SCORE] {sink_recv}")
            self.data_recved += 1
        elif "image_bytes" in sink_recv:
            logging.info(f"* * * [IMAGE BYTES] {sink_recv.keys()}")
            self.data_recved += 1
        elif "status" in sink_recv:
            self.__callback_status(ch, method, properties, body)
        else: 
            logging.info(f"* * * [STUDENTS] {len(sink_recv)}")
            self.data_recved += 1

    def __callback_status(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        
        #if self.client_id != sink_recv["recv_client_id"]:
        #    self.conn_status_send.send(body)
        #    logging.info("Another client connected")
        
        logging.info(f"status: {sink_recv}")
        if sink_recv["status"] == "FINISH":
            self.data_to_recv = sink_recv["data"]
            if self.data_recved == self.data_to_recv:
                logging.info(f"[CLOSE CLIENT]")
                self.alive.value = False
                self.exit_gracefully()

        elif sink_recv["status"] == "BUSY":
            logging.info("System is busy, try later...")
            self.exit_gracefully()
        #    else:
        #self.conn_status_send.send(body)
        #        logging.info("Another client connected")

        elif sink_recv["status"] == "AVAILABLE":
            self.alive.value = True

        elif sink_recv["status"] == "PENDING":
            logging.info("System hasn't finish yet...")

    def get_status(self):
        for method, properties, body in self.channel_status.consume(self.response_queue, inactivity_timeout=TIMEOUT):
            if body != None:
                msg = json.loads(body)
                if "status" in msg:
                    self.channel.basic_ack(method.delivery_tag)
                    return msg
                self.channel.basic_ack(method.delivery_tag)

    def get_response(self, callback):
        for method, properties, body in self.channel_response.consume(self.response_queue, inactivity_timeout=TIMEOUT):
            if body != None:
                callback(self.channel, method, properties, body)
                self.channel.basic_ack(method.delivery_tag)
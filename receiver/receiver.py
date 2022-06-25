import logging
import signal
import csv
import json
import sys
from multiprocessing import Process
from common.connection import Connection


class Receiver:
    def __init__(self, comments_queue, posts_queue, send_workers_comments,
        send_workers_posts, recv_post_queue, recv_comments_queue):
        #self.send_workers_comments = send_workers_comments
        #self.send_workers_posts = send_workers_posts

        #self.students_recved = []
        self.client_conn_recv = Connection(queue_name=recv_post_queue)
        #self.client_conn_recv_c = Connection(queue_name=recv_comments_queue)
        #self.conn_comments = Connection(queue_name=comments_queue)
        self.conn_posts = Connection(queue_name=posts_queue)

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_posts.close()
        #self.conn_comments.close()
        sys.exit(0)

    def start(self):
        self.client_conn_recv.recv(self.__callback_post)
        #self.conn_comments.recv(self.__callback)

    def __callback_post(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logging.info(f"* * * [RECEIVER END] {recv}")
        else:
            logging.info(f"* * * [RECEIVER RECV] {recv}")
            self.conn_posts.send(json.dumps(recv))

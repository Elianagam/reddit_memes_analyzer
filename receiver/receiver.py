import logging
import signal
import json
import sys
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class Receiver(MonitoredMixin):
    def __init__(self, comments_queue, posts_queue, send_workers_comments,
        send_workers_posts, recv_post_queue, recv_comments_queue, send_response_queue,
        students_queue, avg_queue, image_queue, status_response_queue, status_check_queue,
        recv_workers_students
    ):
        self.send_workers_comments = send_workers_comments
        self.send_workers_posts = send_workers_posts
        self.total_end = 2 + recv_workers_students
        self.count_end = 0
        self.actual_client = None
        self.data_to_send = 0

        # CLIENT RECV REQUEST
        self.client_conn_recv = Connection(queue_name=recv_post_queue)
        self.client_conn_recv_c = Connection(queue_name=recv_comments_queue, conn=self.client_conn_recv)

        # CLIENT SEND RESPONSE
        self.client_conn_send = Connection(queue_name=send_response_queue, conn=self.client_conn_recv)

        # SYSTEM RECV
        self.students_recved = []
        self.conn_recv_students = Connection(queue_name=students_queue, conn=self.client_conn_recv)
        self.conn_recv_avg = Connection(exchange_name=avg_queue, routing_key='receiver_avg', bind=True, conn=self.client_conn_recv)
        self.conn_recv_image = Connection(queue_name=image_queue, conn=self.client_conn_recv)

        self.conn_status_recv = Connection(queue_name=status_check_queue, conn=self.client_conn_recv)

        # SYSTEM SEND
        self.conn_comments = Connection(exchange_name=comments_queue, exchange_type='topic')
        self.conn_posts = Connection(exchange_name=posts_queue, exchange_type='topic')

        signal.signal(signal.SIGTERM, self.exit_gracefully)
        super().__init__()

    def exit_gracefully(self, *args):
        self.client_conn_recv.close()
        self.conn_posts.close()
        self.conn_comments.close()
        self.mon_exit()
        sys.exit(0)

    def start(self):
        self.mon_start()
        self.client_conn_recv.recv(self.__callback_post, start_consuming=False)
        self.client_conn_recv_c.recv(self.__callback_comment, start_consuming=False)

        self.conn_recv_students.recv(self.__callback, start_consuming=False)
        self.conn_recv_avg.recv(self.__callback, start_consuming=False)
        self.conn_recv_image.recv(self.__callback, start_consuming=False)
        logging.info("START CONSUMING...")
        self.conn_status_recv.recv(self.__callback_status)

    def __callback_post(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logging.info(f"* * * [RECEIVER POST END] {recv}")
            for i in range(self.send_workers_posts):
                key = i + 1
                worker_key = f"{key}"
                self.conn_posts.send(json.dumps(recv), routing_key=worker_key)
        else:
            self.conn_posts.send(json.dumps(recv))
            key = ((hash(body) % self.send_workers_posts) + 1)
            worker_key = f"{key}"
            self.conn_posts.send(json.dumps(recv), routing_key=worker_key)

    def __callback_comment(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logging.info(f"* * * [RECEIVER COMMENTS END] {recv}")
            for i in range(self.send_workers_comments):
                key = i+1
                worker_key = f"{key}"
                self.conn_comments.send(json.dumps(recv), routing_key=worker_key)
        else:
            self.conn_comments.send(json.dumps(recv))
            key = ((hash(body) % self.send_workers_comments) + 1)
            worker_key = f"{key}"
            self.conn_comments.send(json.dumps(recv), routing_key=worker_key)

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)

        if not "image_bytes" in sink_recv:
            if not "end" in sink_recv:
                logging.info(f"RECV: {sink_recv}")
        else:
            logging.info(f"RECV: {sink_recv.keys()}")

        if not "end" in sink_recv:
            self.data_to_send += 1
            self.client_conn_send.send(json.dumps(sink_recv))
        if "end" in sink_recv:
            self.count_end += 1
            logging.info(f"RECV: {self.count_end} ends")
            if self.total_end == self.count_end:
                logging.info(f"*** RECV ALL END... FINISH")
                self.client_conn_send.send(json.dumps({"status": "FINISH", "data": self.data_to_send}))
                self.count_end = 0
                self.data_to_send = 0
                self.actual_client = None


    def __callback_status(self, ch, method, properties, body):
        recv = json.loads(body)
        # recv 1 avg_score and 1 bytes_image
        msg = {}

        if self.count_end == self.total_end:
            msg = {"status": "FINISH"}
            self.actual_client = None
            self.count_end = 0
        elif self.count_end == 0:
            if self.actual_client == None:
                self.actual_client = recv['client_id']
                msg = {"status": "AVAILABLE"}
            else:
                msg = {"status": "BUSY"}
        else:
            if self.actual_client == recv['client_id']:
                msg = {"status": "PENDING"}
            else:
                msg = {"status": "BUSY"}
        logging.info(f"STATUS: {recv} - response: {msg['status']}")
        self.client_conn_send.send(json.dumps(msg))

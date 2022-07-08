import os
import signal
import json
import sys

from atomicwrites import atomic_write

from common.connection import Connection
from common.health_check.monitored import MonitoredMixin
from common.utils import logger


class Receiver(MonitoredMixin):
    def __init__(self, comments_queue, posts_queue, send_workers_comments,
        send_workers_posts, recv_post_queue, recv_comments_queue, send_response_queue,
        students_queue, avg_queue, image_queue, status_response_queue, status_check_queue,
        recv_workers_students
    ):
        self.send_workers_comments = send_workers_comments
        self.send_workers_posts = send_workers_posts
        self.total_end = 2 + recv_workers_students
        self.msg_hash = []
        self.finish = [False] * self.total_end
        self.actual_client = None
        self.data_to_send = 0

        # CLIENT RECV REQUEST
        self.client_conn_recv = Connection(queue_name=recv_post_queue)
        self.client_conn_recv_c = Connection(queue_name=recv_comments_queue, conn=self.client_conn_recv)

        # CLIENT SEND RESPONSE
        self.client_conn_send = Connection(queue_name=send_response_queue, conn=self.client_conn_recv)

        # SYSTEM RECV
        self.conn_recv_students = Connection(queue_name=students_queue, conn=self.client_conn_recv)
        self.conn_recv_avg = Connection(exchange_name=avg_queue, routing_key='receiver_avg', bind=True, conn=self.client_conn_recv)
        self.conn_recv_image = Connection(queue_name=image_queue, conn=self.client_conn_recv)

        self.conn_status_recv = Connection(queue_name=status_check_queue, conn=self.client_conn_recv)

        # SYSTEM SEND
        self.conn_comments = Connection(exchange_name=comments_queue, exchange_type='topic')
        self.conn_posts = Connection(exchange_name=posts_queue, exchange_type='topic')

        signal.signal(signal.SIGTERM, self.exit_gracefully)
        super().__init__()

        self.__load_state()

    def exit_gracefully(self, *args):
        self.client_conn_recv.close()
        self.conn_posts.close()
        self.conn_comments.close()
        self.mon_exit()
        sys.exit(0)

    def start(self):
        logger.info("Starting")
        self.mon_start()
        self.client_conn_recv.recv(self.__callback_post, start_consuming=False, auto_ack=False)
        self.client_conn_recv_c.recv(self.__callback_comment, start_consuming=False, auto_ack=False)

        self.conn_recv_students.recv(self.__callback, start_consuming=False, auto_ack=False)
        self.conn_recv_avg.recv(self.__callback, start_consuming=False, auto_ack=False)
        self.conn_recv_image.recv(self.__callback, start_consuming=False, auto_ack=False)
        self.conn_status_recv.recv(self.__callback_status, auto_ack=False)

    def __load_state(self):
        if os.path.exists('./data_base/receiver_state.txt'):
            with open('./data_base/receiver_state.txt') as f:
                self.data_to_send = int(f.readline().rstrip('\n'))

                actual_client = f.readline().rstrip('\n')
                if "None" == actual_client:
                    self.actual_client = None

                self.finish = json.loads(f.readline().rstrip('\n'))
                self.msg_hash = json.loads(f.readline())

    def __store_state(self):
        store = f"{self.data_to_send}\n{self.actual_client}\n{json.dumps(self.finish)}\n{json.dumps(self.msg_hash)}"
        with atomic_write('./data_base/receiver_state.txt', overwrite=True) as f:
            f.write(store)

    def __callback_post(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logger.info(f"* * * [RECEIVER POST END] {recv}")
            for i in range(self.send_workers_posts):
                key = i + 1
                worker_key = f"{key}"
                self.conn_posts.send(json.dumps(recv), routing_key=worker_key)
        else:
            self.conn_posts.send(json.dumps(recv))
            key = ((hash(body) % self.send_workers_posts) + 1)
            worker_key = f"{key}"
            self.conn_posts.send(json.dumps(recv), routing_key=worker_key)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __callback_comment(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logger.info(f"* * * [RECEIVER COMMENTS END] {recv}")
            for i in range(self.send_workers_comments):
                key = i+1
                worker_key = f"{key}"
                self.conn_comments.send(json.dumps(recv), routing_key=worker_key)
        else:
            self.conn_comments.send(json.dumps(recv))
            key = ((hash(body) % self.send_workers_comments) + 1)
            worker_key = f"{key}"
            self.conn_comments.send(json.dumps(recv), routing_key=worker_key)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)

        if not "image_bytes" in sink_recv:
            if not "end" in sink_recv:
                logger.info(f"RECV: {sink_recv}")
        else:
            logger.info(f"RECV: {sink_recv.keys()}")

        if not "end" in sink_recv:
            msg_hash = hash(body)
            if msg_hash not in self.msg_hash:
                self.data_to_send += 1
                self.msg_hash.append(msg_hash)
                self.client_conn_send.send(json.dumps(sink_recv))
                self.__store_state()
        if "end" in sink_recv:
            if self.data_to_send != 0:
                if int(sink_recv["end"]) > 0:
                    self.finish[int(sink_recv["end"]) - 1] = True
                else:
                    self.finish[self.total_end + int(sink_recv["end"])] = True
                logger.info(f"RECV: {self.finish} ends")
                self.__store_state()
                if False not in self.finish:
                    logger.info(f"*** RECV ALL END... FINISH")
                    self.client_conn_send.send(json.dumps({"status": "FINISH", "data": self.data_to_send}))
                    self.finish = [False] * self.total_end
                    self.data_to_send = 0
                    self.actual_client = None
                    self.msg_hash = []
                    self.__store_state()

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def __callback_status(self, ch, method, properties, body):
        recv = json.loads(body)
        # recv 1 avg_score and 1 bytes_image
        msg = {}

        if False not in self.finish:
            msg = {"status": "FINISH"}
            self.actual_client = None
            self.finish = [False] * self.total_end
            self.msg_hash = []
            self.__store_state()
        elif True not in self.finish:
            if self.actual_client == None:
                self.actual_client = recv['client_id']
                self.__store_state()
                msg = {"status": "AVAILABLE"}
            else:
                msg = {"status": "BUSY"}
        else:
            if self.actual_client == recv['client_id']:
                msg = {"status": "PENDING"}
            else:
                msg = {"status": "BUSY"}
        logger.info(f"STATUS: {recv} - response: {msg['status']}")
        self.client_conn_send.send(json.dumps(msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)

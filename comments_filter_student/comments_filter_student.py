import signal
import logging

import json
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class CommentsFilterStudent(MonitoredMixin):
    def __init__(self, queue_recv, queue_send, recv_workers, worker_num):
        self.conn_recv = Connection(exchange_name=queue_recv, bind=True, exchange_type='topic', routing_key=f"{worker_num}")
        self.conn_send = Connection(exchange_name=queue_send, exchange_type='topic')
        self.worker_num = worker_num
        self.recv_workers = recv_workers
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.mon_exit()
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        self.mon_start()
        self.conn_recv.recv(self.__callback, auto_ack=False)

    def __callback(self, ch, method, properties, body):
        comments = json.loads(body)

        if "end" in comments:
            for i in range(self.recv_workers):
                key = i + 1
                worker_key = f"{key}"
                self.conn_send.send(json.dumps({"end": self.worker_num}), routing_key=worker_key)
        else:
            result = self.__parser(comments)
            key = ((hash(body) % self.recv_workers) + 1)
            worker_key = f"{key}"
            self.conn_send.send(json.dumps(result), routing_key=worker_key)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __parser(self, comments):
        student_comments = []
        for comment in comments:
            if self.__filter_student(comment):
                comment_new = {
                    "url": comment["url"],
                    "score": comment["score"]
                }
                student_comments.append(comment_new)
        logging.info(f"[STUDENTS TO SEND] {len(student_comments)}")
        return student_comments

    def __filter_student(self, comment):
        student_words = ["university", "college", "student", "teacher", "professor"]
        body = " ".join(comment["body"])
        return any(word.lower() in body for word in student_words)

import signal
import logging

import json
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class CommentsFilterStudent(MonitoredMixin):
    def __init__(self, queue_recv, queue_send, recv_workers):
        self.conn_recv = Connection(queue_name=queue_recv)
        self.conn_send = Connection(queue_name=queue_send)
        self.recv_workers = recv_workers
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.mon_exit()
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        self.mon_start()
        self.conn_recv.recv(self.__callback)

    def __callback(self, ch, method, properties, body):
        comments = json.loads(body)

        if "end" in comments:
            self.conn_send.send(json.dumps(comments))
            return
        else:
            result = self.__parser(comments)
            self.conn_send.send(json.dumps(result))

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
        body =  " ".join(comment["body"])
        return any(word.lower() in body for word in student_words)

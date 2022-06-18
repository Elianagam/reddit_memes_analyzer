import logging
import signal

import json
from common.connection import Connection


class PostsFilterScoreGteAvg:
    def __init__(self, queue_recv_avg, queue_recv_students, queue_send, chunksize=10):
        self.conn_recv_students = Connection(queue_name=queue_recv_students)
        self.conn_recv_avg = Connection(exchange_name=queue_recv_avg, bind=True, conn=self.conn_recv_students)
        self.conn_send = Connection(queue_name=queue_send)
        self.avg_score = None
        self.arrived_early = []
        self.chunksize = chunksize
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv_avg.close()
        self.conn_send.close()

    def start(self):
        self.conn_recv_avg.recv(self.__callback_avg, start_consuming=False)
        self.conn_recv_students.recv(self.__callback_students)

    def __callback_students(self, ch, method, properties, body):
        posts = json.loads(body)
        if "end" in posts:
            self.conn_send.send(json.dumps(posts))
            return

        if self.avg_score != None:
            self.__parser(posts)
        else:
            self.arrived_early.append([post for post in posts])

    def __callback_avg(self, ch, method, properties, body):
        avg = json.loads(body)
        
        if "end" in avg:
            logging.info(f"[AVG END] {self.avg_score}")
            self.__send_arrive_early()
            return
        if "posts_score_avg" in avg:
            self.avg_score = float(avg["posts_score_avg"])

    def __parser(self, posts):
        list_posts = []
        for post in posts:
            if float(post["score"]) >= self.avg_score:
                list_posts.append({"url": post["url"]})

        if len(list_posts) != 0:
            logging.info(f"[STUDENT TO SEND] {list_posts}")
            self.conn_send.send(json.dumps(list_posts))

    def __send_arrive_early(self):
        n = self.chunksize
        lst = self.arrived_early
        chunks = [lst[i:i + n] for i in range(0, len(lst), n)]
        for chunk in chunks:
            logging.info(f"[chunks] {chunks}")
            self.__parser(chunk)

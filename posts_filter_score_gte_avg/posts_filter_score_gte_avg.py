import logging
import os
import signal

import json
from common.connection import Connection
from atomicwrites import atomic_write


class PostsFilterScoreGteAvg:
    def __init__(self, queue_recv_avg, queue_recv_students, queue_send, worker_num, chunksize=10):
        self.conn_recv_students = Connection(queue_name=queue_recv_students)
        self.conn_recv_avg = Connection(exchange_name=queue_recv_avg, bind=True, conn=self.conn_recv_students)
        self.conn_send = Connection(queue_name=queue_send)
        self.chunksize = chunksize
        self.worker_num = worker_num

        self.avg_score = None
        self.arrived_early = []

        self.__load_state()

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv_avg.close()
        self.conn_send.close()

    def start(self):
        self.conn_recv_avg.recv(self.__callback_avg, start_consuming=False, auto_ack=False)
        self.conn_recv_students.recv(self.__callback_students, auto_ack=False)

    def __load_state(self):
        self.arrived_early = []
        if os.path.exists(f'./data_base/post_filter_gte_avg_arrived_early_{self.worker_num}.txt'):
            with open(f'./data_base/post_filter_gte_avg_arrived_early_{self.worker_num}.txt') as f:
                self.arrived_early = json.loads(f.read())

            logging.info(f"loaded: {len(self.arrived_early)} dearly arrive")

        if os.path.exists(f'./data_base/post_filter_gte_avg_avg_{self.worker_num}.txt'):
            with open(f'./data_base/post_filter_gte_avg_avg_{self.worker_num}.txt') as f:
                avg = f.readline()
                if "None" != avg:
                    self.avg_score = float(avg)

            logging.info(f"loaded: {self.avg_score} avg")

    def __store_avg(self):
        store = "{}".format(self.avg_score)
        with atomic_write(f'./data_base/post_filter_gte_avg_avg_{self.worker_num}.txt', overwrite=True) as f:
            f.write(store)

    def __store_arrived_early(self):
        with atomic_write(f'./data_base/post_filter_gte_avg_arrived_early_{self.worker_num}.txt', overwrite=True) as f:
            f.write(json.dumps(self.arrived_early))

    def __callback_students(self, ch, method, properties, body):
        posts = json.loads(body)
        if "end" in posts:
            self.conn_send.send(json.dumps(posts))
        elif self.avg_score is not None:
            self.__parser(posts)
        else:
            for post in posts:
                self.arrived_early.append(post)
            self.__store_arrived_early()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __callback_avg(self, ch, method, properties, body):
        avg = json.loads(body)
        
        if "end" in avg:
            logging.info(f"[AVG END] {self.avg_score}")
            self.__send_arrive_early()
        elif "posts_score_avg" in avg:
            self.avg_score = float(avg["posts_score_avg"])
            self.__store_avg()

        ch.basic_ack(delivery_tag=method.delivery_tag)

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
        logging.info(self.arrived_early)
        chunks = [lst[i:i + n] for i in range(0, len(lst), n)]
        for chunk in chunks:
            logging.info(f"[chunks] {chunks}")
            self.__parser(chunk)

        self.arrived_early = []
        self.__store_arrived_early()

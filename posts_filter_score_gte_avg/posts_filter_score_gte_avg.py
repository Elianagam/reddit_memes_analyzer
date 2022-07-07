import logging
import os
import signal

import json
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin
from atomicwrites import atomic_write


class PostsFilterScoreGteAvg(MonitoredMixin):
    def __init__(self, queue_recv_avg, queue_recv_students, queue_send, worker_num, recv_workers, chunksize=10):
        self.conn_recv_students = Connection(exchange_name=queue_recv_students, bind=True, exchange_type='topic', routing_key=f"{worker_num}")
        self.conn_recv_avg = Connection(exchange_name=queue_recv_avg, bind=True, conn=self.conn_recv_students)
        self.conn_send = Connection(queue_name=queue_send)
        self.chunksize = chunksize
        self.worker_num = worker_num
        self.recv_workers = recv_workers

        self.avg_score = None
        self.arrived_early = []
        self.finish = [False] * recv_workers

        self.__load_state()

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.mon_exit()
        self.conn_recv_avg.close()
        self.conn_send.close()

    def start(self):
        self.mon_start()
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
                avg = f.readline().rstrip('\n')
                if "None" != avg:
                    self.avg_score = float(avg)

                self.finish = json.loads(f.readline())

            logging.info(f"loaded: {self.avg_score} avg, finish: {self.finish}")

    def __store_state(self):
        store = "{}\n{}".format(self.avg_score, json.dumps(self.finish))
        with atomic_write(f'./data_base/post_filter_gte_avg_avg_{self.worker_num}.txt', overwrite=True) as f:
            f.write(store)

    def __store_arrived_early(self):
        with atomic_write(f'./data_base/post_filter_gte_avg_arrived_early_{self.worker_num}.txt', overwrite=True) as f:
            f.write(json.dumps(self.arrived_early))

    def __callback_students(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            if self.avg_score is not None and len(self.arrived_early) == 0:
                self.finish[int(posts["end"]) - 1] = True
                logging.info(self.finish)
                self.__store_state()
                if False not in self.finish:
                    self.conn_send.send(json.dumps({"end": self.worker_num}))
                    self.finish = [False] * self.recv_workers
                    self.avg_score = None
                    self.__store_state()
            elif self.avg_score is None and len(self.arrived_early) != 0:
                self.finish[int(posts["end"]) - 1] = True
                logging.info(self.finish)
                self.__store_state()
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
        elif "posts_score_avg" in avg:
            self.avg_score = float(avg["posts_score_avg"])
            self.__send_arrive_early()
            if False not in self.finish:
                self.conn_send.send(json.dumps({"end": self.worker_num}))
                self.finish = [False] * self.recv_workers
                self.avg_score = None
            self.__store_state()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __parser(self, posts):
        logging.info(f"parser: {posts}")

        list_posts = []

        for post in posts:
            if float(post["score"]) >= self.avg_score:
                list_posts.append({"url": post["url"]})

        if len(list_posts) != 0:
            logging.info(f"[STUDENT TO SEND] {list_posts}")
            self.conn_send.send(json.dumps(list_posts))

    def __send_arrive_early(self):
        n = self.chunksize
        logging.info(f"arrived_early: {len(self.arrived_early)}")
        chunks = []
        for i in range(0, len(self.arrived_early), n):
            chunks = self.arrived_early[i:i+n]
        for chunk in chunks:
            logging.info(f"[chunks] {chunks}")
            self.__parser(chunk)

        self.arrived_early = []
        self.__store_arrived_early()

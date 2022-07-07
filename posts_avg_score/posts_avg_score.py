import os
import signal
import logging
import time

import json

from atomicwrites import atomic_write
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class PostsAvgScore(MonitoredMixin):
    def __init__(self, queue_recv, queue_send, recv_workers):
        self.conn_recv = Connection(queue_name=queue_recv)
        self.conn_send = Connection(exchange_name=queue_send)

        self.posts_ids = []
        self.sum_score = 0
        self.recv_workers = recv_workers
        self.end_recv = [False] * recv_workers

        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.__load_state()
        super().__init__()

    def __load_state(self):
        if os.path.exists("./data_base/avg_score_join_clean.txt"):
            self.__clear_old_state()
            return

        self.posts_ids = []
        if os.path.exists('./data_base/avg_state.txt'):
            with open('./data_base/avg_state.txt') as f:
                sum_score = f.readline().rstrip('\n')
                self.sum_score = float(sum_score)

                for post_id in f:
                    self.posts_ids.append(post_id.rstrip('\n'))

        logging.info(f"loaded: {len(self.posts_ids)} posts_ids, sum_score {self.sum_score}")

        if self.posts_ids:
            if os.path.exists('./data_base/avg_end_recv.txt'):
                with open('./data_base/avg_end_recv.txt') as f:
                    self.end_recv = json.loads(f.read())

        logging.info(f"end_recv: {self.end_recv}")

    def __store_state(self):
        store = f"{self.sum_score}\n"

        for post_id in self.posts_ids:
            store = store + f"{post_id}\n"

        with atomic_write('./data_base/avg_state.txt', overwrite=True) as f:
            f.write(store)

    def __store_end_recv(self):
        with atomic_write('./data_base/avg_end_recv.txt', overwrite=True) as f:
            f.write(json.dumps(self.end_recv))

    def __clear_old_state(self):
        self.end_recv = [False] * self.recv_workers
        self.sum_score = 0
        self.posts_ids = []

        with atomic_write("./data_base/avg_score_join_clean.txt", overwrite=True) as f:
            f.write("True")

        self.__store_state()
        self.__store_end_recv()

        os.remove("./data_base/avg_score_join_clean.txt")

    def exit_gracefully(self, *args):
        self.mon_exit()
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        self.mon_start()
        self.conn_recv.recv(self.__callback, auto_ack=False)
        self.exit_gracefully()

    def __callback(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            if len(self.posts_ids) != 0:
                self.end_recv[int(posts["end"]) - 1] = True
                self.__store_state()
                if False not in self.end_recv:
                    avg = self.__calculate_avg()

                    self.conn_send.send(json.dumps({"posts_score_avg": avg}))
                    self.conn_send.send(json.dumps({"end": -1}))

                    self.__clear_old_state()
        else:
            self.__sum_score(posts)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __sum_score(self, posts):
        for post in posts:
            if post["post_id"] not in self.posts_ids:
                self.sum_score += post["score"]
                self.posts_ids.append(post["post_id"])
        self.__store_state()

    def __calculate_avg(self):
        avg = self.sum_score / len(self.posts_ids)

        logging.info(f" --- [POST_SCORE_AVG] {avg}")
        return avg

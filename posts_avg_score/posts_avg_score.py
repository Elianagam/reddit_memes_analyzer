import os
import signal
import logging

import json

from atomicwrites import atomic_write
from common.connection import Connection


class PostsAvgScore:
    def __init__(self, queue_recv, queue_send, recv_workers):
        self.conn_recv = Connection(queue_name=queue_recv)
        self.conn_send = Connection(exchange_name=queue_send)
        self.posts_ids = []
        self.sum_score = 0
        self.recv_workers = recv_workers
        self.end_recv = 0
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.__load_state()

    def __load_state(self):
        self.posts_ids = []
        if os.path.exists('./data_base/avg_state.txt'):
            with open('./data_base/avg_state.txt') as f:
                sum_score = f.readline()
                self.sum_score = int(sum_score)

                for post_id in f:
                    self.posts_ids.append(post_id.rstrip('\n'))

        logging.info(f"loaded: {len(self.posts_ids)} posts_ids, sum_score {self.sum_score}")

        if self.posts_ids:
            if os.path.exists('./data_base/avg_end_recv.txt'):
                with open('./data_base/avg_end_recv.txt') as f:
                    end_recv = f.readline()
                    self.end_recv = int(end_recv)

        logging.info(f"end_recv: {self.end_recv}")

    def __store_state(self):
        store = f"{self.sum_score}\n"

        for post_id in self.posts_ids:
            store = store + f"{post_id}\n"

        with atomic_write('./data_base/avg_state.txt', overwrite=True) as f:
            f.write(store)

    def __store_end_recv(self):
        store = f"{self.end_recv}\n"

        with atomic_write('./data_base/avg_end_recv.txt', overwrite=True) as f:
            f.write(store)

    def exit_gracefully(self, *args):
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        if self.end_recv == self.recv_workers:
            avg = self.__calculate_avg()

            self.conn_send.send(json.dumps({"posts_score_avg": avg}))
            self.conn_send.send(json.dumps({"end": True}))

            self.end_recv = 0
            self.sum_score = 0
            self.posts_ids = []

            self.__store_state()
            self.__store_end_recv()

        self.conn_recv.recv(self.__callback, auto_ack=False)
        self.exit_gracefully()

    def __callback(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            self.end_recv += 1
            self.__store_end_recv()
            if self.end_recv == self.recv_workers:
                avg = self.__calculate_avg()

                self.conn_send.send(json.dumps({"posts_score_avg": avg}))
                self.conn_send.send(json.dumps(posts))

                self.end_recv = 0
                self.sum_score = 0
                self.posts_ids = []

                self.__store_state()
                self.__store_end_recv()
        else:
            self.__sum_score(posts)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __sum_score(self, posts):
        for post in posts:
            self.sum_score += post["score"]
            self.posts_ids.append(post["post_id"])
        self.__store_state()

    def __calculate_avg(self):
        avg = self.sum_score / len(self.posts_ids)

        logging.info(f" --- [POST_SCORE_AVG] {avg}")
        return avg

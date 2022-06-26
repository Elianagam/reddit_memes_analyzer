import signal
import logging

import json
from common.connection import Connection


class PostsAvgScore:
    def __init__(self, queue_recv, queue_send, recv_workers):
        self.conn_recv = Connection(queue_name=queue_recv)
        self.conn_send = Connection(exchange_name=queue_send)
        self.count_posts = 0 
        self.sum_score = 0
        self.recv_workers = recv_workers
        self.end_recv = 0
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        self.conn_recv.recv(self.__callback)
        self.exit_gracefully()

    def __callback(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            self.end_recv += 1
            if self.end_recv == self.recv_workers:
                avg = self.__calculate_avg()
                self.conn_send.send(json.dumps({"posts_score_avg": avg}))
                self.conn_send.send(json.dumps(posts))
                self.end_recv = 0
        else:
            self.__sum_score(posts)

    def __sum_score(self, posts):
        for post in posts:
            self.sum_score += post["score"]
            self.count_posts += 1

    def __calculate_avg(self):
        avg = self.sum_score / self.count_posts
        
        logging.info(f" --- [POST_SCORE_AVG] {avg}")
        return avg
        

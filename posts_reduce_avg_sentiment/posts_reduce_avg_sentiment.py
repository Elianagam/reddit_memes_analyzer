import logging
import signal

import json
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class PostsAvgSentiment(MonitoredMixin):
    def __init__(self, queue_recv, queue_send, worker_num):
        self.worker_num = worker_num
        self.conn_recv = Connection(exchange_name=queue_recv, bind=True, exchange_type='topic', routing_key=f"{worker_num}")
        self.conn_send = Connection(queue_name=queue_send)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        super().__init__()

    def exit_gracefully(self, *args):
        self.conn_recv.close()
        self.conn_send.close()
        self.mon_exit()

    def __callback(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            self.conn_send.send(json.dumps({"end": self.worker_num}))
        else:
            result = self.__parser(posts)
            self.conn_send.send(json.dumps(result))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.mon_start()
        self.conn_recv.recv(self.__callback, auto_ack=False)

    def __parser(self, posts):
        list_posts = []
        for post in posts:
            sentiments = [float(sentiment) for sentiment in post["sentiments"] if (sentiment != '' and sentiment != None)]
            if len(sentiments) == 0: continue
            post_stm_avg = sum(sentiments) / len(sentiments)
            post_new = {
                "url": post["url"],
                "avg_sentiment": post_stm_avg
            }
            list_posts.append(post_new)
        return list_posts

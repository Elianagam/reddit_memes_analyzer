import os
import signal
import logging

import json
from common.connection import Connection
from atomicwrites import atomic_write


class PostsMaxAvgSentiment:
    def __init__(self, queue_recv, queue_send, recv_workers):
        self.conn_recv = Connection(queue_name=queue_recv)
        self.conn_send = Connection(queue_name=queue_send)

        self.recv_workers = recv_workers
        self.max_avg = {"url": None, "avg_sentiment": 0}
        self.end_recv = 0

        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.__load_state()

    def __load_state(self):
        if os.path.exists('./data_base/post_max_avg_sentiment_state.txt'):
            with open('./data_base/post_max_avg_sentiment_state.txt') as f:
                url = f.readline().rstrip('\n')
                if "None" == url:
                    url = None

                self.max_avg = {
                    "url": url,
                    "avg_sentiment": float(f.readline()),
                }
                self.end_recv = int(f.readline())

    def __store_state(self):
        store = "{}\n{}\n{}".format(self.max_avg["url"], self.max_avg["avg_sentiment"], self.end_recv)
        with atomic_write('./data_base/post_max_avg_sentiment_state.txt', overwrite=True) as f:
            f.write(store)

    def exit_gracefully(self, *args):
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        self.conn_recv.recv(self.__callback, auto_ack=False)

    def __callback(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            self.end_recv += 1
            self.__store_state()

            if self.end_recv == self.recv_workers:
                self.__end_recv(posts)
                self.end_recv = 0
                self.max_avg = {"url": None, "avg_sentiment": 0}
                self.__store_state()
        else:
            self.__get_max_avg_sentiment(posts)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __end_recv(self, end_msg):
        # Send only post with max avg sentiment
        logging.info(f" --- [POST MAX AVG SENTIMENT] {self.max_avg}")

        if self.max_avg["url"] != None:
            download = self.__download_image()
            self.conn_send.send(json.dumps(download))

    def __get_max_avg_sentiment(self, posts):
        for post in posts:
            if post["avg_sentiment"] > self.max_avg["avg_sentiment"] \
                    and post["url"][-3:] in ["png", "jpg"]:
                self.max_avg = post
                self.__store_state()

    def __download_image(self):
        import requests
        import shutil
        import base64

        image_url = self.max_avg["url"]
        filename = "data/max_avg_sentiment.jpg"

        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            response.raw.decode_content = True
            with open(filename, 'wb') as file:
                shutil.copyfileobj(response.raw, file)

            with open(filename, "rb") as image:
                byte_image = bytearray(image.read())
                encoded = base64.b64encode(byte_image)
                data = encoded.decode('ascii')
                logging.info(f"[DOWNLOAD_IMAGE] Success {filename}")
                return {"image_bytes": data}
        else:
            logging.error(f"[DOWNLOAD_IMAGE] Fail")

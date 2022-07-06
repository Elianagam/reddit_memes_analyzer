import signal
import logging

import json
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class PostsMaxAvgSentiment:
    def __init__(self, queue_recv, queue_send, recv_workers):
        self.conn_recv = Connection(queue_name=queue_recv)
        self.conn_send = Connection(queue_name=queue_send)
        self.max_avg = {"url": None, "avg_sentiment": 0}
        self.recv_workers = recv_workers
        self.end_recv = 0
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.mon_exit()
        self.conn_recv.close()
        self.conn_send.close()

    def start(self):
        self.mon_start()
        self.conn_recv.recv(self.__callback)

    def __callback(self, ch, method, properties, body):
        posts = json.loads(body)

        if "end" in posts:
            self.end_recv += 1
            if self.end_recv == self.recv_workers:
                self.__end_recv(posts)
                self.end_recv = 0
        else:
            self.__get_max_avg_sentiment(posts)

    def __end_recv(self, end_msg):
        # Send only post with max avg sentiment
        logging.info(f" --- [POST MAX AVG SENTIMENT] {self.max_avg}")

        if self.max_avg["url"] != None:
            download = self.__download_image()
            self.conn_send.send(json.dumps(download))

        #self.conn_send.send(json.dumps(end_msg))

    def __get_max_avg_sentiment(self, posts):
        for post in posts:
            if post["avg_sentiment"] > self.max_avg["avg_sentiment"] \
                and post["url"][-3:] in ["png", "jpg"]:
                self.max_avg = post

    def __download_image(self):
        import requests 
        import shutil
        import base64

        image_url = self.max_avg["url"]
        filename = "data/max_avg_sentiment.jpg"

        response = requests.get(image_url, stream = True)
        if response.status_code == 200:
            response.raw.decode_content = True
            with open(filename,'wb') as file:
                shutil.copyfileobj(response.raw, file)
            
            with open(filename, "rb") as image:
                byte_image = bytearray(image.read())
                encoded = base64.b64encode(byte_image)
                data = encoded.decode('ascii')  
                logging.info(f"[DOWNLOAD_IMAGE] Success {filename}")
                return {"image_bytes": data}
        else:
            logging.error(f"[DOWNLOAD_IMAGE] Fail")

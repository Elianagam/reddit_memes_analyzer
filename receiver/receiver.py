import logging
import signal
import json
import sys
from common.connection import Connection


class Receiver:
    def __init__(self, comments_queue, posts_queue, send_workers_comments,
        send_workers_posts, recv_post_queue, recv_comments_queue, send_response_queue,
        students_queue, avg_queue, image_queue):
        self.send_workers_comments = send_workers_comments
        self.send_workers_posts = send_workers_posts

        # CLIENT RECV REQUEST
        self.client_conn_recv = Connection(queue_name=recv_post_queue)
        self.client_conn_recv_c = Connection(queue_name=recv_comments_queue, conn=self.client_conn_recv)
        
        # CLIENT SEND RESPONSE
        self.client_conn_send = Connection(queue_name=send_response_queue, conn=self.client_conn_recv)

        # SYSTEM RECV
        self.students_recved = []
        self.conn_recv_students = Connection(queue_name=students_queue, conn=self.client_conn_recv)
        self.conn_recv_avg = Connection(exchange_name=avg_queue, bind=True, conn=self.client_conn_recv)
        self.conn_recv_image = Connection(queue_name=image_queue, conn=self.client_conn_recv)

        # SYSTEM SEND
        self.conn_comments = Connection(exchange_name=comments_queue, exchange_type='topic')
        self.conn_posts = Connection(exchange_name=posts_queue, exchange_type='topic')
        
        self.count_end = 0
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.client_conn_recv.close()
        #self.conn_posts.close()
        #self.conn_comments.close()
        #self.conn_recv_students.close()
        sys.exit(0)

    def start(self):
        self.client_conn_recv.recv(self.__callback_post, start_consuming=False)
        self.client_conn_recv_c.recv(self.__callback_comment, start_consuming=False)
        
        self.conn_recv_students.recv(self.__callback, start_consuming=False)
        self.conn_recv_avg.recv(self.__callback, start_consuming=False)
        logging.info("START CONSUMING...")
        self.conn_recv_image.recv(self.__callback)

    def __callback_post(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logging.info(f"* * * [RECEIVER POST END] {recv}")
            for i in range(self.send_workers_posts):
                key = i + 1
                worker_key = f"{key}"
                self.conn_posts.send(json.dumps(recv), routing_key=worker_key)
        else:
            key = ((hash(body) % self.send_workers_posts) + 1)
            worker_key = f"{key}"
            self.conn_posts.send(json.dumps(recv), routing_key=worker_key)

    def __callback_comment(self, ch, method, properties, body):
        recv = json.loads(body)
        if "end" in recv:
            logging.info(f"* * * [RECEIVER COMMENTS END] {recv}")
            for i in range(self.send_workers_comments):
                key = i+1
                worker_key = f"{key}"
                self.conn_comments.send(json.dumps(recv), routing_key=worker_key)
        else:
            key = ((hash(body) % self.send_workers_comments) + 1)
            worker_key = f"{key}"
            self.conn_comments.send(json.dumps(recv), routing_key=worker_key)

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        try:
            logging.info(f"RECV: {sink_recv.keys()}")   
        except:
            logging.info(f"RECV {len(sink_recv)}")
        finally:
            self.client_conn_send.send(json.dumps(sink_recv))

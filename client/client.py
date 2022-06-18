import logging
import signal
import csv
import json
import sys
from multiprocessing import Process
from common.connection import Connection


class Client:
    def __init__(self, comments_queue, posts_queue, file_comments, 
        file_posts, chunksize, send_workers_comments, send_workers_posts,
        students_queue, avg_queue, image_queue):
        self.file_comments = file_comments
        self.file_posts = file_posts
        self.chunksize = chunksize
        self.send_workers_comments = send_workers_comments
        self.send_workers_posts = send_workers_posts

        self.students_recved = []
        self.conn_posts = Connection(exchange_name=posts_queue, exchange_type='topic')
        self.conn_comments = Connection(exchange_name=comments_queue, exchange_type='topic')

        self.conn_recv_students = Connection(queue_name=students_queue)
        self.conn_recv_avg = Connection(exchange_name=avg_queue, bind=True, conn=self.conn_recv_students)
        self.conn_recv_image = Connection(queue_name=image_queue, conn=self.conn_recv_students)
        
        self.comments_sender = Process(target=self.__send_comments())
        self.posts_sender = Process(target=self.__send_posts())
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logging.info(f"CLOSE RECV CLIENT")
        self.conn_posts.close()
        self.conn_comments.close()
        self.conn_recv_students.close()
        sys.exit(0)


    def start(self):
        logging.info(f"[CLIENT] started...")

        self.posts_sender.run()
        self.comments_sender.run()

        self.__recv_sinks()

        self.comments_sender.join()
        self.posts_sender.join()
        self.exit_gracefully()

    def __recv_sinks(self):
        self.conn_recv_students.recv(self.__callback_students, start_consuming=False)
        self.conn_recv_avg.recv(self.__callback, start_consuming=False)
        self.conn_recv_image.recv(self.__callback)
        

    def __callback_students(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        
        if "end" in sink_recv:
            return
        for student in sink_recv:
            logging.info(f"* * * [CLIENT RECV END STUDENT] {sink_recv}")
            self.students_recved.append(student)
        

    def __callback(self, ch, method, properties, body):
        sink_recv = json.loads(body)
        if "end" in sink_recv:
            return
        else:
            logging.info(f"* * * [CLIENT RECV] {sink_recv.keys()}")

    def __send_comments(self):
        fields = ["type","id", "subreddit.id", "subreddit.name",
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "body", "sentiment", "score"]

        self.__read(self.file_comments, self.conn_comments, fields, self.send_workers_comments)
        self.__send_end(self.conn_comments, self.send_workers_comments)

    def __send_posts(self):
        fields = ["type", "id", "subreddit.id", "subreddit.name", 
                  "subreddit.nsfw", "created_utc", "permalink", 
                  "domain", "url", "selftext", "title", "score"]

        self.__read(self.file_posts, self.conn_posts, fields, self.send_workers_posts)
        self.__send_end(self.conn_posts, self.send_workers_posts)

    def __send_end(self, conn, send_workers):
        for i in range(send_workers):
            worker_key = self.__get_routing_key(i, send_workers)
            logging.info(f"[WORKER_KEY END] {worker_key}")
            conn.send(json.dumps({"end": True}), routing_key=worker_key)

    def __get_routing_key(self, num, send_workers):
        return f"worker.num{(num % send_workers) + 1}"

    def __read(self, file_name, conn, fields, send_workers):
        with open(file_name, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            chunk = []
            count = 0
            for i, line in enumerate(reader):
                if (i % self.chunksize == 0 and i > 0):
                    worker_key = self.__get_routing_key(count, send_workers)
                    logging.info(f"[READ CLIENT] {file_name} {len(chunk)} {worker_key}")
                    conn.send(body=json.dumps(chunk), routing_key=worker_key)
                    chunk = []
                    count += 1
                chunk.append(line)
            
            if len(chunk) != 0:
                worker_key = self.__get_routing_key(count, send_workers)
                logging.info(f"[READ CLIENT] {file_name} {len(chunk)} {worker_key}")
                conn.send(body=json.dumps(chunk), routing_key=worker_key)

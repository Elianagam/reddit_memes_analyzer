import logging
import signal
import csv
import json
import sys
from multiprocessing import Process
from common.connection import Connection


class Client:
	def __init__(file_comments, file_posts, comments_queue, posts_queue,
		students_queue, avg_queue, image_queue, chunksize, host, port):
		self.file_comments = file_comments
		self.file_posts = file_posts
		self.chunksize = chunksize

		self.conn_posts = Connection(queue_name=posts_queue, host=host, port=port)
		#self.conn_comments = Connection(queue_name=comments_queue, host=host, port=port)

		#self.students_recved = []
		#self.conn_recv_students = Connection(queue_name=students_queue)
		#self.conn_recv_avg = Connection(queue_name=avg_queue, conn=self.conn_recv_students)
		#self.conn_recv_image = Connection(queue_name=image_queue, conn=self.conn_recv_students)

		#self.comments_sender = Process(target=self.__send_comments())
		self.posts_sender = Process(target=self.__send_posts())
		signal.signal(signal.SIGTERM, self.exit_gracefully)

	def exit_gracefully(self, *args):
		self.conn_posts.close()
		#self.conn_comments.close()
		self.conn_recv_students.close()
		sys.exit(0)


	def start(self):
		self.posts_sender.run()
		#self.comments_sender.run()

		self.__recv_sinks()

		#self.comments_sender.join()
		self.posts_sender.join()
		self.exit_gracefully()

	def __send_comments(self):
		fields = ["type","id", "subreddit.id", "subreddit.name",
				  "subreddit.nsfw", "created_utc", "permalink", 
				  "body", "sentiment", "score"]

		self.__read(self.file_comments, self.conn_comments, fields)

	def __send_posts(self):
		fields = ["type", "id", "subreddit.id", "subreddit.name", 
				  "subreddit.nsfw", "created_utc", "permalink", 
				  "domain", "url", "selftext", "title", "score"]

		self.__read(self.file_posts, self.conn_posts, fields)


	def __read(self, file_name, conn, fields):
		with open(file_name, mode='r') as csv_file:
			reader = csv.DictReader(csv_file)
			chunk = []
			for i, line in enumerate(reader):
				if (i % self.chunksize == 0 and i > 0):

					conn.send(body=json.dumps(chunk))
					chunk = []
				chunk.append(line)
			
			if len(chunk) != 0:
				conn.send(body=json.dumps(chunk))
"""
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
"""
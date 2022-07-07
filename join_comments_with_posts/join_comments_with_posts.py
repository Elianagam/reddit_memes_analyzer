import os
import signal
import logging
import json

from atomicwrites import atomic_write
from common.connection import Connection
from common.health_check.monitored import MonitoredMixin


class JoinCommentsWithPosts(MonitoredMixin):
    def __init__(self, queue_recv_comments, queue_recv_post, queue_send_students,
                 queue_send_sentiments, chunksize, recv_workers_comments, recv_workers_posts, send_workers):
        self.conn_recv_pst = Connection(queue_name=queue_recv_post)
        self.conn_recv_cmt = Connection(queue_name=queue_recv_comments, conn=self.conn_recv_pst)

        self.conn_send_st = Connection(exchange_name=queue_send_students, exchange_type='topic')
        self.conn_send_se = Connection(exchange_name=queue_send_sentiments, exchange_type='topic')

        self.chunksize = chunksize

        self.join_dict = {}
        self.msg_hash_list = []
        self.finish = {"posts": [False] * recv_workers_posts, "comments": [False] * recv_workers_comments}
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.recv_workers_comments = recv_workers_comments
        self.recv_workers_posts = recv_workers_posts
        self.send_workers = send_workers

        self.__load_state()
        super().__init__()

    def __load_state(self):
        if os.path.exists("./data_base/join_clean"):
            self.__clear_old_state()
            return

        for file in os.listdir("data_base/join_msgs/"):
            path = f"data_base/join_msgs/{file}"
            if file == '.gitignore':
                continue

            with open(path) as f:
                msg = f.read()
                msg_hash = hash(msg)
                decoded_msg = json.loads(msg)
                if file.startswith("p_"):
                    self.__add_post(decoded_msg, msg_hash)
                elif file.startswith("c_"):
                    self.__add_comments(decoded_msg, msg_hash)

        if os.path.exists('./data_base/join_finish'):
            with open('./data_base/join_finish', 'r') as finish_file:
                self.finish = json.loads(finish_file.read())

    def __store_finish(self):
        with atomic_write('./data_base/join_finish', overwrite=True) as finish_file:
            finish_file.write(json.dumps(self.finish))

    def __store_msg(self, msg, type):
        store = json.dumps(msg)

        if type == "p":
            path = f"./data_base/join_msgs/p_{hash(store)}.txt"
        else:
            path = f"./data_base/join_msgs/c_{hash(store)}.txt"

        with atomic_write(path, overwrite=True) as f:
            f.write(store)

    def __clear_old_state(self):
        self.join_dict = {}
        self.msg_hash_list = []
        self.finish = {"posts": [False] * self.recv_workers_posts, "comments": [False] * self.recv_workers_comments}

        with atomic_write("./data_base/join_clean", overwrite=True) as f:
            f.write("True")

        for i in range(self.send_workers):
            key = i + 1
            worker_key = f"{key}"
            self.__send_data(json.dumps({"end": True}), worker_key)

        directory = './data_base/join_msgs'
        for f in os.listdir(directory):
            os.remove(os.path.join(directory, f))

        self.__store_finish()

        os.remove("./data_base/join_clean")

    def exit_gracefully(self, *args):
        self.mon_exit()
        self.conn_recv_pst.close()
        self.conn_send_st.close()
        self.conn_send_se.close()

    def start(self):
        self.mon_start()
        self.conn_recv_cmt.recv(self.__callback_recv_comments, start_consuming=False, auto_ack=False)
        self.conn_recv_pst.recv(self.__callback_recv_posts, auto_ack=False)

    def __callback_recv_comments(self, ch, method, properties, body):
        comments = json.loads(body)

        if not self.__finish(my_key="comments", other_key="posts", readed=comments,
                             my_workers=self.recv_workers_comments,
                             other_workers=self.recv_workers_posts,
                             channel=ch,
                             method=method):
            msg_hash = hash(body)
            if msg_hash not in self.msg_hash_list:
                self.__add_comments(comments, msg_hash)
                self.__store_msg(json.loads(body), "c")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __callback_recv_posts(self, ch, method, properties, body):
        posts = json.loads(body)

        if not self.__finish(my_key="posts", other_key="comments", readed=posts,
                             my_workers=self.recv_workers_posts,
                             other_workers=self.recv_workers_comments,
                             channel=ch,
                             method=method):
            msg_hash = hash(body)
            if msg_hash not in self.msg_hash_list:
                self.__add_post(posts, msg_hash)
                self.__store_msg(json.loads(body), "p")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __finish(self, my_key, other_key, readed, my_workers, other_workers, channel, method):
        if "end" in readed:
            if len(self.msg_hash_list) == 0:
                return True

            self.finish[my_key][int(readed["end"]) - 1] = True
            print(f"""[FINISH JOIN ALL?] {self.finish} | Comments_w: {self.recv_workers_comments} - Posts_w: {self.recv_workers_posts}""")
            logging.info(
                f"""[FINISH JOIN ALL?] {self.finish} | Comments_w: {self.recv_workers_comments} - Posts_w: {self.recv_workers_posts}""")
            self.__store_finish()
            if False not in self.finish[other_key] \
                    and False not in self.finish[my_key]:
                print("FINISH JOIN ALL")
                self.__send_join_data()
                # Send end msg to n workers
                self.__clear_old_state()
            return True
        return False

    def __send_data(self, data, key):
        self.conn_send_st.send(data, routing_key=key)
        self.conn_send_se.send(data, routing_key=key)

    def __add_comments(self, list_comments, msg_hash):
        for c in list_comments:
            key = c["post_id"]
            # get or create dict with key=post_id
            self.join_dict[key] = self.join_dict.get(key, {})

            self.join_dict[key]["body"] = self.join_dict[key].get("body", [])
            self.join_dict[key]["sentiments"] = self.join_dict[key].get("sentiments", [])

            self.join_dict[key]["body"].append(c["body"])
            self.join_dict[key]["sentiments"].append(c["sentiment"])

        self.msg_hash_list.append(msg_hash)

    def __add_post(self, list_posts, msg_hash):
        for p in list_posts:
            key = p["post_id"]
            if key in self.join_dict:
                self.join_dict[key].update(p)
            else:
                self.join_dict[key] = p
                self.join_dict[key]["body"] = []
                self.join_dict[key]["sentiments"] = []

        self.msg_hash_list.append(msg_hash)

    def __send_join_data(self):
        chunk = []
        dic_list = sorted(self.join_dict.items(), key=lambda tup: tup[0])

        for post_id, post in dic_list:
            if not "url" in self.join_dict[post_id]:
                continue
            if len(chunk) == self.chunksize:
                data = json.dumps(chunk)
                key = ((hash(data) % self.send_workers) + 1)
                worker_key = f"{key}"
                self.__send_data(data, worker_key)
                chunk = []

            chunk.append(post)

        # send last data in chunk
        if len(chunk) > 0:
            data = json.dumps(chunk)
            key = ((hash(data) % self.send_workers) + 1)
            worker_key = f"{key}"
            self.__send_data(data, worker_key)

import sys
import logging
import os

from receiver import Receiver
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["POSTS_QUEUE", "COMMETS_QUEUE",
            "SEND_WORKERS_COMMENTS", "SEND_WORKERS_POSTS", "RECV_POSTS_QUEUE", 
            "RECV_COMMENTS_QUEUE", "STUDENTS_QUEUE", "AVG_QUEUE", "IMAGE_QUEUE",
            "SEND_RESPONSE_QUEUE", "STATUS_RESPONSE_QUEUE", "STATUS_CHECK_QUEUE",
            "RECV_WORKERS_STUDENTS"])
        initialize_log()

        logging.info("Client configuration: {}".format(config_params))
        recv = Receiver(
            comments_queue=config_params["COMMETS_QUEUE"],
            posts_queue=config_params["POSTS_QUEUE"],
            send_workers_comments=int(config_params["SEND_WORKERS_COMMENTS"]),
            send_workers_posts=int(config_params["SEND_WORKERS_POSTS"]),
            recv_post_queue=config_params["RECV_POSTS_QUEUE"],
            recv_comments_queue=config_params["RECV_COMMENTS_QUEUE"],
            send_response_queue=config_params["SEND_RESPONSE_QUEUE"],
            students_queue=config_params["STUDENTS_QUEUE"],
            avg_queue=config_params["AVG_QUEUE"],
            image_queue=config_params["IMAGE_QUEUE"],
            status_response_queue=config_params["STATUS_RESPONSE_QUEUE"],
            status_check_queue=config_params["STATUS_CHECK_QUEUE"],
            recv_workers_students=int(config_params["RECV_WORKERS_STUDENTS"]),
        )
        
        recv.start()
    except Exception as e:
        print(f"Close Connection {e}")
        sys.exit(0)


if __name__ == "__main__":
    main()
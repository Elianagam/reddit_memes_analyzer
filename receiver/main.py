import sys
import logging
import os

from receiver import Receiver
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["POSTS_QUEUE", "COMMETS_QUEUE",
            "SEND_WORKERS_COMMENTS", "SEND_WORKERS_POSTS", "RECV_POSTS_QUEUE", "RECV_COMMENTS_QUEUE"])
        initialize_log()

        logging.debug("Client configuration: {}".format(config_params))
        recv = Receiver(
            comments_queue=config_params["COMMETS_QUEUE"],
            posts_queue=config_params["POSTS_QUEUE"],
            send_workers_comments=int(config_params["SEND_WORKERS_COMMENTS"]),
            send_workers_posts=int(config_params["SEND_WORKERS_POSTS"]),
            recv_post_queue=config_params["RECV_POSTS_QUEUE"],
            recv_comments_queue=config_params["RECV_COMMENTS_QUEUE"],
        )
        
        recv.start()
    except Exception as e:
        logging.info(f"Close Connection {e}")
        sys.exit(0)


if __name__ == "__main__":
    main()
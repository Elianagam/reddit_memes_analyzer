import sys
import logging
import os

from client import Client
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["FILE_COMMETS", "FILE_POSTS", "CHUNKSIZE",
            "POSTS_QUEUE", "COMMETS_QUEUE", "SEND_WORKERS_COMMENTS", "SEND_WORKERS_POSTS",
            "STUDENTS_QUEUE", "AVG_QUEUE", "IMAGE_QUEUE"])
        initialize_log()

        logging.debug("Client configuration: {}".format(config_params))
        try:
            print(os.listdir("data"))
        except:
            pass
        client = Client(
            comments_queue=config_params["COMMETS_QUEUE"],
            posts_queue=config_params["POSTS_QUEUE"],
            file_comments=config_params["FILE_COMMETS"],
            file_posts=config_params["FILE_POSTS"],
            chunksize=int(config_params["CHUNKSIZE"]),
            send_workers_comments=int(config_params["SEND_WORKERS_COMMENTS"]),
            send_workers_posts=int(config_params["SEND_WORKERS_POSTS"]),
            students_queue=config_params["STUDENTS_QUEUE"],
            avg_queue=config_params["AVG_QUEUE"],
            image_queue=config_params["IMAGE_QUEUE"],
        )
        client.start()
    except Exception as e:
        sys.exit(0)
        logging.info(f"Close Connection")


if __name__ == "__main__":
    main()
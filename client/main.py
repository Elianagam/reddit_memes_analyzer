import sys
import logging
import os

from client import Client
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config([
            "FILE_POSTS",
            "CHUNKSIZE",
            "POSTS_QUEUE",
            "COMMENTS_QUEUE",
            "FILE_COMMENTS",
            "STUDENTS_QUEUE",
            "AVG_QUEUE",
            "IMAGE_QUEUE",
        ])
        initialize_log()

        logging.info("Client configuration: {}".format(config_params))

        client = Client(
            file_posts=config_params["FILE_POSTS"],        
            posts_queue=config_params["POSTS_QUEUE"],
            chunksize=int(config_params["CHUNKSIZE"]),
            file_comments=config_params["FILE_COMMENTS"],
            comments_queue=config_params["COMMENTS_QUEUE"],
            students_queue=config_params["STUDENTS_QUEUE"],
            avg_queue=config_params["AVG_QUEUE"],
            image_queue=config_params["IMAGE_QUEUE"],

        )
        client.start()
    except Exception as e:
        logging.info(f"Close Connection {e}")
        #sys.exit(0)


if __name__ == "__main__":
    main()
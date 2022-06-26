import sys
import logging
import os

from client import Client
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config([
        #    "FILE_COMMETS",
            "FILE_POSTS",
            "CHUNKSIZE",
            "POSTS_QUEUE",
           # "COMMETS_QUEUE",
            "HOST", "PORT",
             #"STUDENTS_QUEUE", "AVG_QUEUE", "IMAGE_QUEUE"
        ])
        initialize_log()

        logging.info("Client configuration: {}".format(config_params))
        client = Client(
            #comments_queue=config_params["COMMETS_QUEUE"],
            posts_queue=config_params["POSTS_QUEUE"],
            #file_comments=config_params["FILE_COMMETS"],
            file_posts=config_params["FILE_POSTS"],
            chunksize=int(config_params["CHUNKSIZE"]),
            #students_queue=config_params["STUDENTS_QUEUE"],
            #avg_queue=config_params["AVG_QUEUE"],
            #image_queue=config_params["IMAGE_QUEUE"],
            host=config_params["HOST"],
            port=config_params["PORT"],
        )
        client.start()
    except Exception as e:
        logging.info(f"Close Connection")
        sys.exit(0)


if __name__ == "__main__":
    main()
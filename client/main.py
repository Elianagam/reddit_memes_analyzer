import sys
import logging
import os

from client import Client
from common.utils import initialize_log, initialize_config


def main():
    config_params = initialize_config([
        "FILE_POSTS",
        "CHUNKSIZE",
        "POSTS_QUEUE",
        "COMMENTS_QUEUE",
        "FILE_COMMENTS",
        "RESPONSE_QUEUE",
        "STATUS_CHECK_QUEUE",
        "STATUS_RESPONSE_QUEUE",
        "CLIENT_ID"
    ])
    initialize_log()

    logging.info("Client configuration: {}".format(config_params))

    client = Client(
        file_posts=config_params["FILE_POSTS"],        
        posts_queue=config_params["POSTS_QUEUE"],
        chunksize=int(config_params["CHUNKSIZE"]),
        file_comments=config_params["FILE_COMMENTS"],
        comments_queue=config_params["COMMENTS_QUEUE"],
        response_queue=config_params["RESPONSE_QUEUE"],
        status_check_queue=config_params["STATUS_CHECK_QUEUE"],
        status_response_queue=config_params["STATUS_RESPONSE_QUEUE"],
        client_id=config_params["CLIENT_ID"],
    )
    client.start()
   


if __name__ == "__main__":
    main()
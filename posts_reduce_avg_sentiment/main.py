import logging

from posts_reduce_avg_sentiment import PostsAvgSentiment
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "WORKER_NUM"])
        initialize_log()

        logging.info("Server configuration: {}".format(config_params))

        recver = PostsAvgSentiment(config_params["QUEUE_RECV"], config_params["QUEUE_SEND"], config_params["WORKER_NUM"])
        recver.start()
    except Exception as e:
        logging.info(f"Close Connection")


if __name__ == "__main__":
    main()
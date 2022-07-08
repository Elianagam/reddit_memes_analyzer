from posts_reduce_avg_sentiment import PostsAvgSentiment
from common.utils import initialize_log, initialize_config, logger


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "WORKER_NUM"])
        initialize_log()

        logger.debug("Server configuration: {}".format(config_params))
        logger.info("Initializing")

        recver = PostsAvgSentiment(config_params["QUEUE_RECV"], config_params["QUEUE_SEND"], config_params["WORKER_NUM"])
        recver.start()
    except Exception as e:
        logger.exception("Something Happened")
        logger.info(f"Close Connection")


if __name__ == "__main__":
    main()

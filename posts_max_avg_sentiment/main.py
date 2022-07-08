import traceback
from posts_max_avg_sentiment import PostsMaxAvgSentiment
from common.utils import initialize_log, initialize_config, logger


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "RECV_WORKERS"])
        initialize_log()

        logger.debug("Server configuration: {}".format(config_params))
        logger.info("Initializing")

        recver = PostsMaxAvgSentiment(
            config_params["QUEUE_RECV"],
            config_params["QUEUE_SEND"],
            int(config_params["RECV_WORKERS"])
        )
        recver.start()
    except Exception as e:
        logger.exception("Something Happened")
        logger.info(f"Close Connection")


if __name__ == "__main__":
    main()

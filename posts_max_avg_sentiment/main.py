import logging
import traceback
from posts_max_avg_sentiment import PostsMaxAvgSentiment
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "RECV_WORKERS"])
        initialize_log()

        logging.info("Server configuration: {}".format(config_params))

        recver = PostsMaxAvgSentiment(
            config_params["QUEUE_RECV"],
            config_params["QUEUE_SEND"],
            int(config_params["RECV_WORKERS"])
        )
        recver.start()
    except Exception as e:
        print("PERNO ERROR, ", traceback.format_exc())
        logging.info(f"Close Connection")

if __name__ == "__main__":
    main()

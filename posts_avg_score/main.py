import logging

from posts_avg_score import PostsAvgScore
from common.utils import initialize_log, initialize_config


def main():
    #try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "RECV_WORKERS"])
        initialize_log()

        logging.info("Server configuration: {}".format(config_params))

        recver = PostsAvgScore(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send=config_params["QUEUE_SEND"],
            recv_workers=int(config_params["RECV_WORKERS"])
        )
        recver.start()
    #except Exception as e:
    #    logging.info(f"Close Connection {e}")


if __name__ == "__main__":
    main()
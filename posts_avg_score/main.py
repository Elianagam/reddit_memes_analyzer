from posts_avg_score import PostsAvgScore
from common.utils import initialize_log, initialize_config, logger


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "RECV_WORKERS"])
        initialize_log()

        logger.debug("Server configuration: {}".format(config_params))
        logger.info("Initializing")

        recver = PostsAvgScore(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send=config_params["QUEUE_SEND"],
            recv_workers=int(config_params["RECV_WORKERS"])
        )
        recver.start()
    except Exception as e:
        logger.exception("Something Failed")
        logger.info(f"Close Connection {e}")


if __name__ == "__main__":
    main()

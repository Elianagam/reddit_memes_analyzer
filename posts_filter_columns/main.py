from posts_filter_columns import PostsFilterColumns
from common.utils import logger, initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND_JOIN",
            "QUEUE_SEND_AVG", "WORKER_NUM"])
        initialize_log()

        logger.info("Initializing Filter Columns")
        logger.debug("Server configuration: %r", config_params)

        recver = PostsFilterColumns(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send_to_join=config_params["QUEUE_SEND_JOIN"],
            queue_send_to_avg=config_params["QUEUE_SEND_AVG"],
            worker_num=config_params["WORKER_NUM"],
        )
        recver.start()
    except Exception as e:
        logger.info(f"Close Connection {e}")


if __name__ == "__main__":
    main()

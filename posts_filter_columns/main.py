import logging

from posts_filter_columns import PostsFilterColumns
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND_JOIN",
            "QUEUE_SEND_AVG", "WORKER_KEY"])
        initialize_log()

        logging.info("Server configuration: {}".format(config_params))

        recver = PostsFilterColumns(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send_to_join=config_params["QUEUE_SEND_JOIN"],
            queue_send_to_avg=config_params["QUEUE_SEND_AVG"],
            worker_key=int(config_params["WORKER_KEY"])
        )
        recver.start()
    except Exception as e:
        logging.info(f"Close Connection")


if __name__ == "__main__":
    main()
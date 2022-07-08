from comments_filter_columns import CommentsFilterColumns
from common.utils import initialize_log, initialize_config, logger


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "WORKER_NUM"])
        initialize_log()

        logger.debug("Server configuration: {}".format(config_params))
        logger.info("Initializing Comments Filter Column")

        recver = CommentsFilterColumns(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send=config_params["QUEUE_SEND"],
            worker_num=config_params["WORKER_NUM"]
        )
        recver.start()
    except Exception as e:
        logger.exception("Something Happened")
        logger.info(f"Close Connection")


if __name__ == "__main__":
    main()

import traceback

from comments_filter_student import CommentsFilterStudent
from common.utils import initialize_log, initialize_config, logger


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "RECV_WORKERS", "WORKER_NUM"])
        initialize_log()

        logger.debug("Server configuration: {}".format(config_params))
        logger.info("Initializing Comments Filter Student")
        recver = CommentsFilterStudent(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send=config_params["QUEUE_SEND"],
            recv_workers=int(config_params["RECV_WORKERS"]),
            worker_num=config_params["WORKER_NUM"]
        )
        recver.start()
    except Exception as e:
        logger.exception("An error occurred")
        logger.info(f"Close Connection")


if __name__ == "__main__":
    main()

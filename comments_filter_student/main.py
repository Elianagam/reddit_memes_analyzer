import logging
import traceback

from comments_filter_student import CommentsFilterStudent
from common.utils import initialize_log, initialize_config


def main():
    print("PERNO Iniciando el COMMENTS FILTER")
    try:
        print("PERNO inicializando config")
        config_params = initialize_config(["QUEUE_RECV", "QUEUE_SEND", "RECV_WORKERS", "WORKER_NUM"])
        print("PERNO inicializando log")
        initialize_log()

        logging.info("Server configuration: {}".format(config_params))
        print("PERNO llamando al constructor")
        recver = CommentsFilterStudent(
            queue_recv=config_params["QUEUE_RECV"],
            queue_send=config_params["QUEUE_SEND"],
            recv_workers=int(config_params["RECV_WORKERS"]),
            worker_num=config_params["WORKER_NUM"]
        )
        print("PERNO  ANTES DEL START")
        recver.start()
        print("PERNO LUEGO DEL ESTART")
    except Exception as e:
        print("PERNO error", e)
        print("PERNO ERROR ", traceback.format_exc())
        logging.info(f"Close Connection")


if __name__ == "__main__":
    main()

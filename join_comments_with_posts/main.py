import logging

from join_comments_with_posts import JoinCommentsWithPosts
from common.utils import initialize_log, initialize_config


def main():
    try:
        config_params = initialize_config(["QUEUE_RECV_COMMENTS", "QUEUE_RECV_POSTS",
            "QUEUE_SEND_STUDENTS", "QUEUE_SEND_SENTIMENTS", "CHUNKSIZE",
            "RECV_WORKERS_COMMENTS", "RECV_WORKERS_POSTS", "SEND_WORKERS"])
        initialize_log()

        logging.info("Server configuration: {}".format(config_params))

        recver = JoinCommentsWithPosts(
            queue_recv_comments=config_params["QUEUE_RECV_COMMENTS"],
            queue_recv_post=config_params["QUEUE_RECV_POSTS"],
            queue_send_students=config_params["QUEUE_SEND_STUDENTS"],
            queue_send_sentiments=config_params["QUEUE_SEND_SENTIMENTS"],
            chunksize=int(config_params["CHUNKSIZE"]),
            recv_workers_comments=int(config_params["RECV_WORKERS_COMMENTS"]),
            recv_workers_posts=int(config_params["RECV_WORKERS_POSTS"]),
            send_workers=int(config_params["SEND_WORKERS"])
            )
        recver.start()
    except Exception as e:
        logging.info(f"Close Connection")

if __name__ == "__main__":
    main()
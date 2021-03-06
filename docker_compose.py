import sys

INIT_DOCKER = """version: '3'
services:
  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: Dockerfile
    ports:
      - 15672:15672
      - 5672:5672
    networks:
      - rabbitmq

  receiver:
    container_name: receiver
    image: receiver:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - COMMETS_QUEUE=comments_queue
      - POSTS_QUEUE=posts_queue
      - SEND_WORKERS_COMMENTS={worker_join_comments}
      - SEND_WORKERS_POSTS={worker_join_posts}
      - RECV_POSTS_QUEUE=client_posts_queue
      - RECV_COMMENTS_QUEUE=client_comments_queue
      - SEND_RESPONSE_QUEUE=client_response_queue
      - STUDENTS_QUEUE=student_url_queue
      - AVG_QUEUE=posts_avg_score_queue
      - IMAGE_QUEUE=post_avg_sentiments_queue
      - STATUS_CHECK_QUEUE=client_status_check_queue
      - STATUS_RESPONSE_QUEUE=client_status_response_queue
      - RECV_WORKERS_STUDENTS={filter_exchange}
      - CONTAINER_NAME=receiver
      - LOGLEVEL=DEBUG
    volumes:
      - ./data_base:/data_base
    networks:
      - rabbitmq

  <COMMENTS_FILTER_COLUMNS>
  <COMMENTS_FILTER_STUDENTS>
  <POST_FILTER_SCORE_GTE_AVG>
  <POST_REDUCE_AVG_SENTIMETS>

  posts_max_avg_sentiment:
    container_name: posts_max_avg_sentiment
    image: posts_max_avg_sentiment:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    volumes:
      - ./posts_max_avg_sentiment/data:/data
      - ./data_base:/data_base
    environment:
      - QUEUE_RECV=post_sentiments_queue
      - QUEUE_SEND=post_avg_sentiments_queue
      - RECV_WORKERS={filter_exchange}
      - CONTAINER_NAME=posts_max_avg_sentiment
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq

  <POSTS_FILTER_COLUMNS>

  posts_avg_score:
    container_name: posts_avg_score
    image: posts_avg_score:latest
    entrypoint: python3 /main.py
    restart: on-failure
    volumes:
      - ./data_base:/data_base
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV=posts_for_avg_queue
      - QUEUE_SEND=posts_avg_score_queue
      - RECV_WORKERS={worker_join_posts}
      - CONTAINER_NAME=posts_avg_score
      - PYTHONHASHSEED=0
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq

  join_comments_with_posts:
    container_name: join_comments_with_posts
    image: join_comments_with_posts:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV_COMMENTS=comments_filter_queue
      - QUEUE_RECV_POSTS=posts_for_join_queue
      - QUEUE_SEND_STUDENTS=cmt_pst_join_st_queue
      - QUEUE_SEND_SENTIMENTS=cmt_pst_join_se_queue
      - CHUNKSIZE={chunksize}
      - RECV_WORKERS_COMMENTS={worker_join_comments}
      - RECV_WORKERS_POSTS={worker_join_posts}
      - SEND_WORKERS={filter_exchange}
      - CONTAINER_NAME=join_comments_with_posts
      - PYTHONHASHSEED=0
      - LOGLEVEL=DEBUG
    volumes:
      - ./data_base:/data_base
    networks:
      - rabbitmq

  <HEALTH_CHECK>

networks:
  rabbitmq:
    name: rabbitmq
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

COMENTS_FILTERS = """
  comments_filter_columns_{id}:
    container_name: comments_filter_columns_{id}
    image: comments_filter_columns:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV=comments_queue
      - QUEUE_SEND=comments_filter_queue
      - CONTAINER_NAME=comments_filter_columns_{id}
      - WORKER_NUM={id}
      - PYTHONHASHSEED=0
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq
"""

FILTER_STUDENTS = """
  comments_filter_student_{id}:
    container_name: comments_filter_student_{id}
    image: comments_filter_student:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV=cmt_pst_join_st_queue
      - QUEUE_SEND=posts_student_queue
      - RECV_WORKERS={exchange}
      - CONTAINER_NAME=comments_filter_student_{id}
      - WORKER_NUM={id}
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq
"""

FILTER_SCORE_STUDENTS = """
  posts_filter_score_gte_avg_{id}:
    container_name: posts_filter_score_gte_avg_{id}
    image: posts_filter_score_gte_avg:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV_AVG=posts_avg_score_queue
      - QUEUE_RECV_STUDENTS=posts_student_queue
      - QUEUE_SEND=student_url_queue
      - CHUNKSIZE={chunksize}
      - CONTAINER_NAME=posts_filter_score_gte_avg_{id}
      - WORKER_NUM={id}
      - RECV_WORKERS={filter_exchange}
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq
    volumes:
      - ./data_base:/data_base
"""

POSTS_FILTER = """
  posts_filter_columns_{id}:
    container_name: posts_filter_columns_{id}
    image: posts_filter_columns:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV=posts_queue
      - QUEUE_SEND_JOIN=posts_for_join_queue
      - QUEUE_SEND_AVG=posts_for_avg_queue
      - CONTAINER_NAME=posts_filter_columns_{id}
      - WORKER_NUM={id}
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq
"""

REDUCE_SENTIMETS = """
  posts_reduce_avg_sentiment_{id}:
    container_name: posts_reduce_avg_sentiment_{id}
    image: posts_reduce_avg_sentiment:latest
    entrypoint: python3 /main.py
    restart: on-failure
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - QUEUE_RECV=cmt_pst_join_se_queue
      - QUEUE_SEND=post_sentiments_queue
      - CONTAINER_NAME=posts_reduce_avg_sentiment_{id}
      - WORKER_NUM={id}
      - LOGLEVEL=DEBUG
    networks:
      - rabbitmq
"""


HEALTH_CHECKER = """
  healthchecker:
    image: health_check:latest
    deploy:
      replicas: {replicas}
    environment:
      - REPLICAS={replicas}
      - ELECTION_TIMEOUT=4
      - HEARTBEAT_SLEEP=1
      - HEARTBEAT_TIMEOUT=4
      - SLEEP_SECONDS=1
      - HEALTHCHECK_READ_TIMEOUT=2
      - HEALTHCHECK_NODE_TIMEOUT=6
      - HEALTHBEAT_DELAY=2
      - VICTORY_TIMEOUT=2
      - LOGLEVEL=DEBUG
    depends_on:
      - rabbitmq
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./persistence/:/tmp/persistence/
    networks:
      - rabbitmq
"""

def add_filters(num, init_txt):
  filter_txt = ""
  for i in range(1,num+1):
      filter_txt += init_txt.format(id=i)
  return filter_txt


def main():
    filter_exchange = int(sys.argv[1]) #1
    workers_join_comments = int(sys.argv[2]) #2
    workers_join_posts = int(sys.argv[3]) #3
    chunksize = int(sys.argv[4]) #4
    healthcheck = int(sys.argv[5])


    filters_c = add_filters(workers_join_comments, COMENTS_FILTERS)
    filters_p = add_filters(workers_join_posts, POSTS_FILTER)

    filters_s = ""
    filters_ss = ""
    reduce_se = ""
    health_check_s = HEALTH_CHECKER.format(replicas=healthcheck)

    for x in range(1,filter_exchange+1):
        filters_s += FILTER_STUDENTS.format(id=x, exchange=filter_exchange)
        filters_ss += FILTER_SCORE_STUDENTS.format(id=x, chunksize=chunksize, filter_exchange=filter_exchange)
        reduce_se += REDUCE_SENTIMETS.format(id=x)

    compose = INIT_DOCKER.format(worker_join_comments=workers_join_comments,
                                 worker_join_posts=workers_join_posts,
                                 filter_exchange=filter_exchange,
                                 chunksize=chunksize) \
                  .replace("<COMMENTS_FILTER_COLUMNS>", filters_c) \
                  .replace("<COMMENTS_FILTER_STUDENTS>", filters_s) \
                  .replace("<POST_FILTER_SCORE_GTE_AVG>", filters_ss) \
                  .replace("<POSTS_FILTER_COLUMNS>", filters_p) \
                  .replace("<POST_REDUCE_AVG_SENTIMETS>", reduce_se) \
                  .replace("<HEALTH_CHECK>", health_check_s) \

    with open("docker-compose.yaml", "w") as compose_file:
        compose_file.write(compose)


if __name__ == "__main__":
    main()

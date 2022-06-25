SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

docker-client-image:
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-client-image

docker-client-run:
	# docker run -it --network=reddit_memes_analyzer_rabbitmq "client:latest"
	docker-compose -f docker-compose-client.yaml up -d --build --remove-orphans
.PHONY: docker-client-run

docker-system-image:
	docker build -f ./receiver/Dockerfile -t "receiver:latest" .
	docker build -f ./comments_filter_columns/Dockerfile -t "comments_filter_columns:latest" .
	docker build -f ./comments_filter_student/Dockerfile -t "comments_filter_student:latest" .
	docker build -f ./posts_filter_score_gte_avg/Dockerfile -t "posts_filter_score_gte_avg:latest" .

	docker build -f ./posts_reduce_avg_sentiment/Dockerfile -t "posts_reduce_avg_sentiment:latest" .
	docker build -f ./posts_max_avg_sentiment/Dockerfile -t "posts_max_avg_sentiment:latest" .

	docker build -f ./posts_filter_columns/Dockerfile -t "posts_filter_columns:latest" .
	docker build -f ./posts_avg_score/Dockerfile -t "posts_avg_score:latest" .
	docker build -f ./join_comments_with_posts/Dockerfile -t "join_comments_with_posts:latest" .
.PHONY: docker-system-image

docker-compose-up:
	docker-compose -f docker-compose.yaml up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml stop -t 2
	docker-compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs
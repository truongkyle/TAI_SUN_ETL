run-all:
	docker compose -f platform/docker-compose.yaml --env-file ./.env up --build -d
down-rm:
	docker compose -f platform/docker-compose.yaml --env-file ./.env down -v
build-spark:
	docker build -t spark-nyc:latest ./spark

JOB ?=nyc_test.py
spark-submit:
	docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/$(JOB)

list-exited:
	docker ps -a | grep Exited 


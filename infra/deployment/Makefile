DATA_WAREHOUSE_INPUT_DATA_FOLDER=/var/lib/clickhouse/user_files

run-datawarehouse:
	export DATA_WAREHOUSE_INPUT_DATA_FOLDER=$(DATA_WAREHOUSE_INPUT_DATA_FOLDER) && \
	docker compose --file=./docker-compose.yml up -d
register-forklift-flows:
	docker pull $(FORKLIFT_DOCKER_IMAGE):$(FORKLIFT_VERSION) && \
	./register-flows.sh

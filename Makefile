# DEV commands
dev-run-data-warehouse:
	git clone --depth=1 --branch=master https://github.com/MTES-MCT/monitorfish.git ./forklift/tests/test_data/external/monitorfish || echo "Monitorfish repository already present - skipping git clone" && \
	export DATA_WAREHOUSE_PASSWORD=password && \
	export DATA_WAREHOUSE_USER=clickhouse_user && \
	docker compose -f ./infra/data_warehouse/docker-compose.yml -f ./infra/data_warehouse/docker-compose-test-data.yml up -d --remove-orphans

dev-run-metabase:
	docker compose -f ./infra/data_warehouse/docker-compose-dev-metabase.yml up -d && \
	docker cp ./infra/metabase_plugins/clickhouse.metabase-driver.jar metabase:/plugins && \
	docker restart metabase

dev-stop-data-warehouse:
	docker compose -f ./infra/data_warehouse/docker-compose.yml -f ./infra/data_warehouse/docker-compose-test-data.yml down

dev-stop-metabase:
	docker compose -f ./infra/data_warehouse/docker-compose-dev-metabase.yml down

dev-erase-data-warehouse-databases:
	docker volume rm data_warehouse_data-warehouse-db data_warehouse_data-warehouse-logs data_warehouse_monitorfish-db-data || exit 0

dev-test-forklift: dev-stop-data-warehouse dev-erase-data-warehouse-databases dev-run-data-warehouse
	cd forklift && export TEST_LOCAL=True && poetry run coverage run -m pytest --pdb --ignore=tests/test_data/external tests/ && poetry run coverage report && poetry run coverage html

# CI commands - Forklift
docker-build-forklift:
	docker build -f "infra/docker/Dockerfile.Forklift" . -t forklift:$(VERSION)
docker-run-data-warehouse:
	git clone --depth=1 --branch=master https://github.com/MTES-MCT/monitorfish.git ./forklift/tests/test_data/external/monitorfish || echo "Monitorfish repository already present - skipping git clone" && \
	export DATA_WAREHOUSE_PASSWORD=password && \
	export DATA_WAREHOUSE_USER=clickhouse_user && \
	docker compose -f ./infra/data_warehouse/docker-compose.yml -f ./infra/data_warehouse/docker-compose-test-data.yml up -d --remove-orphans
docker-test-forklift: docker-run-data-warehouse
	docker run --network host -v /var/run/docker.sock:/var/run/docker.sock -u forklift:$(DOCKER_GROUP) --env-file forklift/.env.test forklift:$(VERSION) coverage run -m pytest --pdb tests
docker-tag-forklift:
	docker tag forklift:$(VERSION) ghcr.io/mtes-mct/monitorfish/forklift:$(VERSION)
docker-push-forklift:
	docker push ghcr.io/mtes-mct/monitorfish/forklift:$(VERSION)

# RUN commands
run-datawarehouse:
	docker compose -f ./infra/data_warehouse/docker-compose.yml up -d
register-forklift-flows:
	docker pull ghcr.io/mtes-mct/monitorfish/forklift:$(FORKLIFT_VERSION) && \
	infra/forklift/register-flows.sh

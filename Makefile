DATA_WAREHOUSE_INPUT_DATA_FOLDER=$(shell pwd)/forklift/tests/test_data/clickhouse_user_files

# DEV commands
# NOTE: `docker compose up -d` returns as soon as containers are STARTED,
# not once one-shot containers (the 3 flyway migration jobs) have actually
# FINISHED. Without an explicit `wait`, the next command in the chain
# (running the tests) can start querying the databases while Flyway is
# still mid-migration -- a silent, deterministic race, not a flaky one:
# whichever migrations haven't been applied yet by the time a given test
# queries them will look like empty tables, with no error surfaced
# anywhere (the flyway containers' own logs were never being captured).
dev-run-data-warehouse:
	git clone --depth=1 --branch=master https://github.com/MTES-MCT/monitorfish.git ./forklift/tests/test_data/external/monitorfish || echo "Monitorfish repository already present - skipping git clone" && \
	git clone --depth=1 --branch=main https://github.com/MTES-MCT/monitorenv.git ./forklift/tests/test_data/external/monitorenv || echo "Monitorenv repository already present - skipping git clone" && \
	git clone --depth=1 --branch=main https://github.com/MTES-MCT/rapportnav2.git ./forklift/tests/test_data/external/rapportnav || echo "RapportNav repository already present - skipping git clone" && \
	export DATA_WAREHOUSE_PASSWORD=password && \
	export DATA_WAREHOUSE_USER=clickhouse_user && \
	export DATA_WAREHOUSE_INPUT_DATA_FOLDER=$(DATA_WAREHOUSE_INPUT_DATA_FOLDER) && \
	export DOCKER_DEFAULT_PLATFORM=linux/amd64 && \
	docker compose -f ./infra/deployment/docker-compose.yml -f ./infra/testing/docker-compose-test-data.yml up -d --remove-orphans && \
	FLYWAY_EXIT_CODES=$$(docker wait monitorfish_flyway monitorenv_flyway rapportnav_flyway); \
	docker compose -f ./infra/deployment/docker-compose.yml -f ./infra/testing/docker-compose-test-data.yml logs monitorfish-flyway monitorenv-flyway rapportnav-flyway; \
	echo "flyway exit codes (monitorfish, monitorenv, rapportnav): $$FLYWAY_EXIT_CODES"; \
	echo "$$FLYWAY_EXIT_CODES" | grep -qv '^0$$' && exit 1 || exit 0

dev-run-metabase:
	docker compose -f ./infra/testing/docker-compose-dev-metabase.yml up -d && \
	docker cp ./infra/metabase_plugins/clickhouse.metabase-driver.jar metabase:/plugins && \
	docker restart metabase

dev-stop-data-warehouse:
	export DATA_WAREHOUSE_PASSWORD=password && \
	export DATA_WAREHOUSE_USER=clickhouse_user && \
	export DATA_WAREHOUSE_INPUT_DATA_FOLDER=$(DATA_WAREHOUSE_INPUT_DATA_FOLDER) && \
	docker compose -f ./infra/deployment/docker-compose.yml -f ./infra/testing/docker-compose-test-data.yml down

dev-stop-metabase:
	docker compose -f ./infra/testing/docker-compose-dev-metabase.yml down

dev-erase-data-warehouse-databases:
	docker volume rm deployment_data-warehouse-db deployment_data-warehouse-logs deployment_monitorfish-db-data deployment_monitorenv-db-data deployment_rapportnav-db-data || exit 0

dev-run-infra-for-tests: dev-stop-data-warehouse dev-erase-data-warehouse-databases dev-run-data-warehouse

dev-test-forklift: dev-run-infra-for-tests
	cd forklift && export TEST_LOCAL=True && poetry run coverage run -m pytest --pdb --ignore=tests/test_data/external tests/ && poetry run coverage report && poetry run coverage html

dev-erase-external-data:
	rm -rf forklift/tests/test_data/external/monitorenv
	rm -rf forklift/tests/test_data/external/monitorfish
	rm -rf forklift/tests/test_data/external/rapportnav

# CI commands - Forklift
docker-build-forklift:
	docker build -f "infra/docker/Dockerfile.Forklift" . -t forklift:$(VERSION)
# See NOTE on dev-run-data-warehouse above: `up -d` doesn't wait for the
# one-shot flyway migration containers to actually finish, so the tests
# were racing against still-in-progress migrations with no visible error.
docker-run-data-warehouse:
	git clone --depth=1 --branch=master https://github.com/MTES-MCT/monitorfish.git ./forklift/tests/test_data/external/monitorfish || echo "Monitorfish repository already present - skipping git clone" && \
	git clone --depth=1 --branch=main https://github.com/MTES-MCT/monitorenv.git ./forklift/tests/test_data/external/monitorenv || echo "Monitorenv repository already present - skipping git clone" && \
	git clone --depth=1 --branch=main https://github.com/MTES-MCT/rapportnav2.git ./forklift/tests/test_data/external/rapportnav || echo "RapportNav repository already present - skipping git clone" && \
	export DATA_WAREHOUSE_PASSWORD=password && \
	export DATA_WAREHOUSE_USER=clickhouse_user && \
	export DATA_WAREHOUSE_INPUT_DATA_FOLDER=$(DATA_WAREHOUSE_INPUT_DATA_FOLDER) && \
	docker compose -f ./infra/deployment/docker-compose.yml -f ./infra/testing/docker-compose-test-data.yml up -d --remove-orphans && \
	FLYWAY_EXIT_CODES=$$(docker wait monitorfish_flyway monitorenv_flyway rapportnav_flyway); \
	docker compose -f ./infra/deployment/docker-compose.yml -f ./infra/testing/docker-compose-test-data.yml logs monitorfish-flyway monitorenv-flyway rapportnav-flyway; \
	echo "flyway exit codes (monitorfish, monitorenv, rapportnav): $$FLYWAY_EXIT_CODES"; \
	echo "$$FLYWAY_EXIT_CODES" | grep -qv '^0$$' && exit 1 || exit 0
docker-test-forklift: docker-run-data-warehouse
	docker run --network host -v /var/run/docker.sock:/var/run/docker.sock -u forklift:$(DOCKER_GROUP) --env-file forklift/.env.test forklift:$(VERSION) coverage run -m pytest --pdb tests
docker-tag-forklift:
	docker tag forklift:$(VERSION) ghcr.io/mtes-mct/fisheries-and-environment-data-warehouse/forklift:$(VERSION)
docker-push-forklift:
	docker push ghcr.io/mtes-mct/fisheries-and-environment-data-warehouse/forklift:$(VERSION)

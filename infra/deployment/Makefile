run-datawarehouse:
	docker compose -f ./docker-compose.yml up -d
register-forklift-flows:
	docker pull ghcr.io/mtes-mct/fisheries-and-environment-data-warehouse/forklift:$(FORKLIFT_VERSION) && \
	./register-flows.sh

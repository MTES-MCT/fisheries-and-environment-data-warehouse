run-datawarehouse:
	docker compose -f ./docker-compose.yml up -d
register-forklift-flows:
	docker pull $(FORKLIFT_DOCKER_IMAGE):$(FORKLIFT_VERSION) && \
	./register-flows.sh

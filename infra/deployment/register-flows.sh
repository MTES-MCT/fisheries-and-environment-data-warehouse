#!/bin/bash

docker run -t --rm --network=host --name forklift-register-flows \
        -v /opt2/sacrois-data:/opt2/sacrois-data \
        -v "$(pwd)"/.env:/home/forklift/forklift/.env \
	    -v "$(pwd)"/backend.toml:/home/forklift/.prefect/backend.toml \
        --env-file .env \
        -e FORKLIFT_VERSION \
        -e FORKLIFT_DOCKER_IMAGE \
        $FORKLIFT_DOCKER_IMAGE:$FORKLIFT_VERSION \
        python forklift/main.py

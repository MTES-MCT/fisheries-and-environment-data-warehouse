#!/bin/bash

docker run -t --rm --network=host --name forklift-register-flows \
        -v /opt2/sacrois-data:/opt2/sacrois-data \
        -v "$(pwd)"/.env:/home/forklift/data_warehouse/.env \
	    -v "$(pwd)"/backend.toml:/home/forklift/.prefect/backend.toml \
        --env-file .env \
        -e FORKLIFT_VERSION \
        -e SACROIS_FILES_GID \
        ghcr.io/mtes-mct/fisheries-and-environment-data-warehouse/forklift:$FORKLIFT_VERSION \
        python forklift/main.py

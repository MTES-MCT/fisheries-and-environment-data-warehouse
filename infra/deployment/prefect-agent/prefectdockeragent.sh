#!/bin/bash
source ~/prefect-agent/.venv/bin/activate && \
source ~/prefect-agent/.prefect-agent && \
prefect agent docker start --api "${PREFECT_SERVER_URL}" --label forklift;

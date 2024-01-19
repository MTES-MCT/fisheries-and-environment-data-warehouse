import os
from pathlib import Path

from dotenv import load_dotenv

# Package structure
ROOT_DIRECTORY = Path(__file__).parent.parent
LIBRARY_LOCATION = ROOT_DIRECTORY / Path("forklift")
QUERIES_LOCATION = LIBRARY_LOCATION / Path("pipeline/queries")
SQL_SCRIPTS_LOCATION = LIBRARY_LOCATION / Path("pipeline/sql_scripts")

# Must be set to true when running tests locally
TEST_LOCAL = os.getenv("TEST_LOCAL", "False").lower() in ("true", "t", "yes", "y")
if TEST_LOCAL:
    load_dotenv(ROOT_DIRECTORY / ".env.test")

# Flow execution configuration
DOCKER_IMAGE = "ghcr.io/mtes-mct/monitorfish/forklift"
FORKLIFT_VERSION = os.getenv("FORKLIFT_VERSION")
FLOWS_LOCATION = Path("forklift/pipeline/flows")  # relative to the WORKDIR in the image
FLOWS_LABEL = "forklift"
MAX_FLOW_RUN_MINUTES = 30
FLOW_STATES_TO_CLEAN = ["Running"]

# Proxies for pipeline flows requiring Internet access
PROXIES = {
    "http": os.environ.get("HTTP_PROXY_"),
    "https": os.environ.get("HTTPS_PROXY_"),
}

# Prefect Server endpoint
PREFECT_SERVER_URL = os.getenv("PREFECT_SERVER_URL")

# data.gouv.fr configuration
DATAGOUV_API_ENDPOINT = "https://www.data.gouv.fr/api/1"
DATAGOUV_API_KEY = os.getenv("DATAGOUV_API_KEY")

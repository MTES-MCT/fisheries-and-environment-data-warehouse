import os
from pathlib import Path

from dotenv import load_dotenv

# Package structure
ROOT_DIRECTORY = Path(__file__).parent.parent
TEST_DATA_LOCATION = ROOT_DIRECTORY / "tests/test_data"
LIBRARY_LOCATION = ROOT_DIRECTORY / Path("forklift")
QUERIES_LOCATION = LIBRARY_LOCATION / Path("pipeline/queries")
SQL_SCRIPTS_LOCATION = LIBRARY_LOCATION / Path("pipeline/sql_scripts")

# Must be set to true when running tests locally
TEST_LOCAL = os.getenv("TEST_LOCAL", "False").lower() in ("true", "t", "yes", "y")
if TEST_LOCAL:
    load_dotenv(ROOT_DIRECTORY / ".env.test")

# Flow execution configuration
FORKLIFT_DOCKER_IMAGE = os.getenv("FORKLIFT_DOCKER_IMAGE")
FORKLIFT_VERSION = os.getenv("FORKLIFT_VERSION")
FLOWS_LOCATION = Path("forklift/pipeline/flows")  # relative to the WORKDIR in the image
FLOWS_LABEL = "forklift"
MAX_FLOW_RUN_MINUTES = 60
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

# Data warehous credentials for use in flows
DATA_WAREHOUSE_USER = os.getenv("DATA_WAREHOUSE_USER")
DATA_WAREHOUSE_PWD = os.getenv("DATA_WAREHOUSE_PWD")

# Rapportnav API
RAPPORTNAV_API_ENDPOINT = "https://rapport-nav.din.developpement-durable.gouv.fr/api/"
RAPPORTNAV_API_KEY = os.getenv("RAPPORTNAV_API_KEY")
## Data Warehouse Deployment
### 1. Purpose and Architecture

This Data Warehouse is meant to collect data from Monitorfish, Monitorenv and Rapportnav V2 applications and provide
a centralized database for statistical analysis through data querying and visualization tools such as Metabase.

It is composed of 2 dockerized services :
- a Clickhouse database
- a set of python data flows orchestrated by Prefect, named `Forklift`

### 2. Deployment
#### 2.1. Prerequisites

- Install docker : https://docs.docker.com/engine/install/debian/#install-using-the-repository
- Create a new user `adduser --disabled-password d_dawar`
- Add user to sudoers : `echo 'd_dawar  ALL=(ALL) NOPASSWD:ALL' | EDITOR='tee -a' visudo`
- Add user to docker group: `usermod -a -G docker d_dawar`
- Switch to user : `su - d_dawar`
- Install GNU make if not already installed :   
    `sudo apt-get update`  
    `sudo apt-get install make`
- Create a `.add_proxies` file to easily add proxy environment variables by sourcing it when needed. The file should contain:
    
      touch .add_proxies
      export http_proxy=http://xxx.xx.xxx.xxxx:pppp >> .add_proxies
      export https_proxy=http://xxx.xx.xxx.xxxx:pppp >> .add_proxies

- Install py_env dependencies

      sudo apt update;
      sudo apt install build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl git libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev;

- Install pyenv :

          # Install
          source ~/.add_proxies && curl https://pyenv.run | bash

          # Post-install config
          echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
          echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
          echo 'eval "$(pyenv init -)"' >> ~/.bashrc
          echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc

          # Log out and login for changes to take effect
          exit
          su - d_dawar


  See https://github.com/pyenv/pyenv-installer for more information.
- Install python : `source ~/.add_proxies && pyenv install 3.12.3`
- Copy the contents of the `infra/deployment` folder onto the host machine, in the `~` folder :

      # Clone repo (shallow copy)
      git clone --depth 1 --single-branch --branch main https://github.com/MTES-MCT/fisheries-and-environment-data-warehouse git-repo

      # Take only the infra/deployment directory from the git repo
      mv git-repo/infra/deployment/* ~

      # Delete the rest of the git repo
      rm -rf git-repo

- Fill placeholders in `.prefect-agent` and `prefectdockeragent.service` :

      # Ajouter l'url de l'orchestrateur Prefect avec /graphql à la fin
      export PREFECT_URL=
      sed -i 's/<USER-TO-CHANGE>/'$USER'/g' prefect-agent/prefectdockeragent.service
      
      # Le `sed` au milieu de la commande permet d'échapper les caractères `/` de l'url en `\/`
      sed -i 's/<URL-TO-CHANGE>/'$(sed 's/\//\\\//g'<<< $PREFECT_URL)'/g' prefect-agent/.prefect-agent

- Create a virtual environment in the `~/prefect-agent` folder and install `prefect` in this virtual environment :
  - `cd prefect-agent`
  - `pyenv local 3.12.3`
  - `python -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install prefect==1.3.0`
  - Run `prefect backend server` to switch Prefect into server mode (as opposed to cloud mode, the default). A file `backend.toml` should appear at `~/.prefect/backend.toml` containing `backend = "server"`.
- Define and start a Prefect agent service:

      sudo mv prefectdockeragent.service /etc/systemd/system
      sudo systemctl enable prefectdockeragent.service
      sudo systemctl start prefectdockeragent.service
- Run `systemctl status prefectdockeragent.service` to check the service is running.
- Rename `.env.template` as `.env` and fill in the environement variables : these are the environment variables that will be used by the flow runners, inside the forklift runner containers.

#### 2.2. Running the Data Warehouse

- Update the `DATA_WAREHOUSE_USER` and `DATA_WAREHOUSE_PASSWORD` environment variables in the `.data-warehouse` file.
- Run `source .data-warehouse`.
- Run `make run-datawarehouse`.

#### 2.3. Running or updating `Forklift`

- Update the `FORKLIFT_VERSION` and `FORKLIFT_DOCKER_IMAGE` variables in the `.data-warehouse` file.
- Run `source .data-warehouse`.
- Run `make register-forklift-flows`.

# Install

## Ubuntu

### build a ubuntu machine to run this
* `docker build . --build-arg password=<password> --build-arg timezone=America/Montreal --build-arg python_version=3.11.6 --build-arg node_version=16.16.0 --build-arg hf_cli_version=1.35.0 -t hf-workbench:latest --no-cache`
Assuming your source folder cross projects is ~whatever/source then project ~whatever/source/hf/repo (Map your volumes as you wish)
This is bash (go fish or not)
* `docker create --name hf-workbench0 -v "$(builtin cd ../..;pwd):/home/ubuntu/source/" -t -i hf-workbench bash`
* `docker start hf-workbench0`

### Virtual env
* Delete venv `pyenv uninstall -f venv`
* Create virtualenv `pyenv virtualenv venv` 
* Activate venv `pyenv activate venv`
* Update pip `pip install -U pip pipenv`
* Install requirements: `pipenv install`
* List existing virtualenvs: `pyenv virtualenvs`
* Deactivate venv `pyenv deactivate`
* More info about using virtualenv can be found [here](https://github.com/pyenv/pyenv-virtualenv#usage)

### VS Code extensions reminder
* Python
* Gitlens

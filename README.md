# Install

## Ubuntu

### build a ubuntu machine to run this
* `docker build . --build-arg password=<password> --build-arg timezone=America/Montreal -t hf-workbench:latest --no-cache `
Assuming your source folder cross projects is ~whatever/source then project ~whatever/source/hf/repo (Map your volumes as you wish)
This is bash (go fish or not)
* `docker create --name hf-workbench0 -v "$(builtin cd ../..;pwd):/home/ubuntu/source/" -t -i hf-workbench bash`
* `docker start hf-workbench0`

### Virtual env
* Create virtualenv `pyenv virtualenv venv` 
* Activate venv `pyenv activate venv`
* Update pip `pip install -U pip pipenv`
* Install requirements: `pipenv install`
* List existing virtualenvs: `pyenv virtualenvs`
* Deactivate venv `pyenv deactivate`
* Delete venv `pyenv uninstall -f venv`
* More info about using virtualenv can be found [here](https://github.com/pyenv/pyenv-virtualenv#usage)

### VS Code extensions reminder
* Python
* Gitlens

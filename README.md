# Install

## Ubuntu

### build a ubuntu machine to run this
* `docker build . --build-arg password=<password> --build-arg timezone=America/Montreal --build-arg python_version=3.11.6 --build-arg node_version=16.16.0 --build-arg hf_cli_version=1.35.0 -t hf-workbench:latest --no-cache`
Assuming your source folder cross projects is ~whatever/source then project ~whatever/source/hf/repo (Map your volumes as you wish)
This is bash (go fish or not)
* `docker create --name hf-workbench0 -v "$(builtin cd ../..;pwd):/home/ubuntu/source/" -t -i hf-workbench bash`
* `docker start hf-workbench0`

### Virtual env and Python install
* Deactivate in case it is already there `source deactivate`
* Delete venv `rm -rf ./venv`
* Create venv `python -m venv venv`
* Activate venv `source venv/bin/activate`
* Update pip `python -m pip install --upgrade pip`
* Install requirements: `python -m pip install -r requirements.txt`
* Activate venv: `source venv/bin/activate`

### Alternatives using pyenv and virtual env.
You don't need to run this, but you can use pyenv instead of venv
* More info about using virtualenv can be found [here](https://github.com/pyenv/pyenv-virtualenv#usage)

### VS Code extensions reminder
* Python
* Gitlens

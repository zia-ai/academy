# Install

## Ubuntu

### build a ubuntu machine to run this
* `docker build . --build-arg password=<password> timezone=America/Montreal -t hf-workbench:latest -n `
Assuming your source folder cross projects is ~whatever/source then project ~whatever/source/hf/repo (Map your volumes as you wish)
This is bash (go fish or not)
* `docker create --name hf-workbench0 -v "$(builtin cd ../..;pwd):/home/ubuntu/source/" -t -i hf-workbench bash`
* `docker start hf-workbench0`

### or just do your dependencies
* You need Python 3.8.x (check `python3 --version`)
* Install deps: `sudo apt install python3.8 python3.8-dev python3-venv`

### Virtual env
* Create virtualenv & activate `python3 -m venv venv` 
* if fish shell `source venv/bin/activate.fish`
* if bash shell `source venv/bin/activate`
* Update pip `pip install -U pip pipenv`
* Install requirements: `pipenv install`

### VS Code extensions reminder
* Python
* Gitlens

## MacOS

### On MacOS
* You need Python 3.8.x (check `python3 --version`)
* Create virtualenv & activate (`python3 -m venv venv` && `source venv/bin/activate.fish`)
* Update pip `pip install -U pip pipenv`
* Install requirements: `pipenv install`

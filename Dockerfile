FROM ubuntu:focal
# Replace shell with bash so we can source files
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN echo "This builds a container with the tools necessary to run node docs repo and python 3.11.6 scripts repo"
ARG password
ARG timezone
RUN test -n "$password" || (echo "password not set" && false)
# https://en.wikipedia.org/wiki/List_of_tz_database_time_zones 
RUN test -n "$timezone" || (echo "timezone not set - common options: timezone=Europe/London or timezone=America/Montreal" && false)
RUN echo "creating docker with password: $password"
ENV TZ=Europe/London
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install base dependencies and python
RUN apt-get update && apt-get install -y -q --no-install-recommends \
        apt-transport-https \
        build-essential \
        ca-certificates \
        curl \
        git \
        libssl-dev \
        wget \
        tzdata \
        sudo \
        software-properties-common \
        libsasl2-dev \
        git \
        openssh-server \
        zlib1g-dev \
        libncurses5-dev \
        libgdbm-dev \
        libnss3-dev \
        libreadline-dev \
        libffi-dev \
        libsqlite3-dev \
        libbz2-dev \
        liblzma-dev \
        tk-dev \
        libdb-dev

# Install HF CLI tool
ARG hf_cli_version
ENV HFVER=$hf_cli_version
RUN wget https://github.com/zia-ai/humanfirst/releases/download/cli-$HFVER/hf-linux-amd64?raw=true -O /usr/local/bin/hf && chmod 755 /usr/local/bin/hf

# This runs user mode user changeover and finalise
RUN useradd --create-home --shell /bin/bash ubuntu
RUN echo ubuntu:$password | chpasswd
RUN usermod -aG sudo ubuntu
USER ubuntu
WORKDIR /home/ubuntu
RUN mkdir source
COPY .bashrc_custom /home/ubuntu/.bashrc

# Install python
ARG python_version
RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv
ENV HOME /home/ubuntu
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
ENV SETUPTOOLS_USE_DISTUTILS=stdlib
RUN pyenv install $python_version
RUN pyenv global $python_version

# Update pip
RUN pip install --upgrade pip
RUN pip install --upgrade pip pipenv
RUN pip install numpy==1.26.4
RUN pip install spacy
RUN python -m spacy download en_core_web_md
RUN python -m spacy download en_core_web_lg

# Install pyenv virtualenv
RUN git clone https://github.com/pyenv/pyenv-virtualenv.git $(pyenv root)/plugins/pyenv-virtualenv

# node - using node version manager
# setup the shell under ubuntu
# Installing Node
# SHELL ["/bin/bash", "--login", ,"-i", "-c"]
# RUN echo run whoami && source /home/ubuntu/.bashrc && curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash && nvm install 16.16.0

ARG node_version
ENV NVM_DIR /home/ubuntu/.nvm
ENV NODE_VERSION $node_version

WORKDIR $NVM_DIR

RUN curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash \
    && . $NVM_DIR/nvm.sh \
    && nvm install $NODE_VERSION \
    && nvm alias default $NODE_VERSION \
    && nvm use default

ENV NODE_PATH $NVM_DIR/versions/node/v$NODE_VERSION/lib/node_modules
ENV PATH      $NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH

# yarn
RUN npm install --global yarn

FROM python:3.8-buster

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /tap_postgres
LABEL python_version=python

RUN pip install nose flake8 pylint pylint-exit black

COPY Makefile requirements.txt ./

RUN make install

COPY . .

CMD ["make", "lint", "test"]

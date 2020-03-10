.PHONY: install lint test docker-test docker-lint docker-ci docker-shell docker-stop

install:
	pip install -r requirements.txt

lint:
	flake8
	pylint tap_postgres || pylint-exit $$?

test:
	nosetests -v

define DOCKER_RUN
docker build -t tap_postgres . && \
docker network create tap_postgres; \
docker run --network="tap_postgres" --name tap_postgres_db -e POSTGRES_PASSWORD=postgres -d postgres; \
docker run -it --rm --env-file .env-test --network="tap_postgres" --name tap_postgres 
endef

docker-test:
	$(DOCKER_RUN) tap_postgres make test

docker-lint:
	$(DOCKER_RUN) tap_postgres make lint

docker-ci:
	$(DOCKER_RUN) tap_postgres make lint test
	make docker-stop

docker-shell:
	$(DOCKER_RUN) --volume ${PWD}:/tap_postgres tap_postgres /bin/bash

docker-format:
	$(DOCKER_RUN) --volume ${PWD}:/tap_postgres tap_postgres black tap_postgres tests

docker-stop:
	docker rm -f tap_postgres_db
	docker network rm tap_postgres
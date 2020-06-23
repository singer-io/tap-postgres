FROM postgres:9.6

# Git SHA of v2.2
ENV WAL2JSON_COMMIT_ID=9f9762315062888f7f7f4f0a115073a33ad1275e

# Compile the plugins from sources and install
RUN apt-get update && apt-get install -y postgresql-server-dev-9.6 gcc git make pkgconf \
    && git clone https://github.com/eulerto/wal2json -b master --single-branch \
    && (cd /wal2json && git checkout $WAL2JSON_COMMIT_ID && make && make install) \
    && rm -rf wal2json

# Copy the custom configuration which will be passed down to the server (using a .sample file is the preferred way of doing it by
# the base Docker image)
COPY postgresql.conf.sample /usr/local/share/postgresql/postgresql.conf.sample

# Copy the script which will initialize the replication permissions
COPY /docker-entrypoint-initdb.d /docker-entrypoint-initdb.d
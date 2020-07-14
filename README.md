# tap-postgres

## set rds.logical_replication in parameter(reboot)= 1

This should also set `max_wal_senders && max_replication_slots > 0`

Singer tap for PostgreSQL supporting Full Table & Logical Replication
using the wal2json decoder plugin.

```
SELECT * FROM pg_create_logical_replication_slot('stitch', 'wal2json');
```

## Configuration
Check out `config.json.sample` for an example configuration file.

| Field                      | Required? | Default | Details                                                                                                                                                                   |
|----------------------------|-----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|                            |           |         |                                                                                                                                                                           |
| dbname                     | Yes       |         | The name of the database to connect to                                                                                                                                    |
| default_replication_method | No        |         | Allows setting a default replication method. If the replication method for a stream is not set in the catalog.json, then this replication method is used. Can be one of ` |
| default_replication_key    | No        |         | Allows setting a default replication key. Should be a string value containing the field name, e.g. `updated_at`                                                           |
| host                       | Yes       |         | The host to be used to connect                                                                                                                                            |
| password                   | Yes       |         | The password to be used to connect                                                                                                                                        |
| port                       | Yes       |         | The port to be used to connect                                                                                                                                            |
| user                       | Yes       |         | The user to be used to connect                                                                                                                                            |
| use_ssh_tunnel             | No        | False   | Set to true to open an SSH tunnel and connect to the database through the tunnel                                                                                          |
| ssh_jump_server            | No        |         | Only used if `use_ssh_tunnel` is set to true. This is the URL or IP address of the jump server that the connection should tunnel through                                  |
| ssh_jump_server_port       | No        | 22      | Only used if `use_ssh_tunnel` is set to true. This is the port of the jump server the SSH tunnel will attempt to connect to                                               |
| ssh_private_key_path       | No        |         | Only used if `use_ssh_tunnel` is set to true. This is the path on the local machine to the private SSH key                                                                |
| ssh_username               | No        |         | Only used if `use_ssh_tunnel` is set to true. This is the username to be used to connect to the jump server                                                               |
| ssl                        | No        |         | If true, the sslmode value is set to "require" otherwise sslmode value is not set                                                                                         |

## Development
### Running tests
Install requirements for development: `pip install -e .[test]`

Install postgres to run tests:
1. sudo apt update
1. sudo apt install postgresql postgresql-contrib
1. sudo service postgresql start
1. sudo passwd postgres (set to postgres)
1. sudo -u postgres psql
1. alter user postgres password 'postgres'
1. \q

Run unit tests: `nosetests --where tests/unittests`



---

Copyright &copy; 2018 Stitch

### tap-postgres


Singer tap for Postgres supporting Full Table & Logical Replication using the wal2json decoder plugin.

 SELECT * FROM pg_create_logical_replication_slot('stitch', 'wal2json'); 

# tap-postgres

## set rds.logical_replication in parameter(reboot)= 1

This should also set `max_wal_senders && max_replication_slots > 0`

Singer tap for PostgreSQL supporting Full Table & Logical Replication
using the wal2json decoder plugin.

```
SELECT * FROM pg_create_logical_replication_slot('stitch', 'wal2json');
```

---

Copyright &copy; 2018 Stitch

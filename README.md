# pg-stat
Simple scripts to keep snapshots of PostgreSQL statistics

## Take snapshot
Execute:
```psql -U tad -h localhost -p 5432 -d mydb -f stats_snapshot.sql```

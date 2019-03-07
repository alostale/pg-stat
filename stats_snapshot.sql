\copy (select * from pg_stat_user_tables) to program 'cat > pg_stat_user_tables_$(date +%F_%T).copy'
\copy (select * from pg_stat_statements) to program 'cat > pg_stat_statements_$(date +%F_%T).copy'

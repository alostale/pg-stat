create table temp_stat_user_tables
(relid                oid                      ,
 schemaname           name                     ,
 relname              name                     ,
 seq_scan             bigint                   ,
 seq_tup_read         bigint                   ,
 idx_scan             bigint                   ,
 idx_tup_fetch        bigint                   ,
 n_tup_ins            bigint                   ,
 n_tup_upd            bigint                   ,
 n_tup_del            bigint                   ,
 n_tup_hot_upd        bigint                   ,
 n_live_tup           bigint                   ,
 n_dead_tup           bigint                   ,
 n_mod_since_analyze  bigint                   ,
 last_vacuum          timestamp with time zone ,
 last_autovacuum      timestamp with time zone ,
 last_analyze         timestamp with time zone ,
 last_autoanalyze     timestamp with time zone ,
 vacuum_count         bigint                   ,
 autovacuum_count     bigint                   ,
 analyze_count        bigint                   ,
 autoanalyze_count    bigint                   );

create table stat_user_tables 
(relid                oid                      ,
 schemaname           name                     ,
 relname              name                     ,
 seq_scan             bigint                   ,
 seq_tup_read         bigint                   ,
 idx_scan             bigint                   ,
 idx_tup_fetch        bigint                   ,
 n_tup_ins            bigint                   ,
 n_tup_upd            bigint                   ,
 n_tup_del            bigint                   ,
 n_tup_hot_upd        bigint                   ,
 n_live_tup           bigint                   ,
 n_dead_tup           bigint                   ,
 n_mod_since_analyze  bigint                   ,
 last_vacuum          timestamp with time zone ,
 last_autovacuum      timestamp with time zone ,
 last_analyze         timestamp with time zone ,
 last_autoanalyze     timestamp with time zone ,
 vacuum_count         bigint                   ,
 autovacuum_count     bigint                   ,
 analyze_count        bigint                   ,
 autoanalyze_count    bigint                   ,
 snapshot_id          int);

create table temp_stat_statements
(
userid	            oid	,
dbid	            oid	,
queryid	            bigint,
query	            text,
calls	            bigint,
total_time	        double precision,
min_time	        double precision,
max_time	        double precision,
mean_time	        double precision,
stddev_time	        double precision,
rows	            bigint,
shared_blks_hit	    bigint,
shared_blks_read	bigint,
shared_blks_dirtied	bigint,
shared_blks_written	bigint,
local_blks_hit	    bigint,
local_blks_read	    bigint,
local_blks_dirtied	bigint,
local_blks_written	bigint,
temp_blks_read	    bigint,
temp_blks_written	bigint,
blk_read_time	    double precision,
blk_write_time	    double precision);

create table stat_statements
(
userid	            oid	,
dbid	            oid	,
queryid	            bigint,
query	            text,
calls	            bigint,
total_time	        double precision,
min_time	        double precision,
max_time	        double precision,
mean_time	        double precision,
stddev_time	        double precision,
rows	            bigint,
shared_blks_hit	    bigint,
shared_blks_read	bigint,
shared_blks_dirtied	bigint,
shared_blks_written	bigint,
local_blks_hit	    bigint,
local_blks_read	    bigint,
local_blks_dirtied	bigint,
local_blks_written	bigint,
temp_blks_read	    bigint,
temp_blks_written	bigint,
blk_read_time	    double precision,
blk_write_time	    double precision,
snapshot_id         int);

create table snapshot 
(
	snapshot_id int primary key,
	ts  timestamp without time zone default now() not null
	customer text,
	description text
)

create or replace view compare_statements as (
select 
s2.calls               - s1.calls               as calls              ,
s2.total_time          - s1.total_time          as total_time         ,
LEAST(s2.min_time , s1.min_time)            as min_time           ,    -- not nesarily from the period
greatest(s2.max_time            , s1.max_time)            as max_time   , -- not nesarily from the period
(s2.total_time - s1.total_time) /(s2.calls-s1.calls)       as mean_time          ,
s2.stddev_time         /*- s1.stddev_time*/         as stddev_time        ,
s2.rows                - s1.rows                as rows               ,
s2.shared_blks_hit     - s1.shared_blks_hit     as shared_blks_hit    ,
s2.shared_blks_read    - s1.shared_blks_read    as shared_blks_read   ,
s2.shared_blks_dirtied - s1.shared_blks_dirtied as shared_blks_dirtied,
s2.shared_blks_written - s1.shared_blks_written as shared_blks_written,
s2.local_blks_hit      - s1.local_blks_hit      as local_blks_hit     ,
s2.local_blks_read     - s1.local_blks_read     as local_blks_read    ,
s2.local_blks_dirtied  - s1.local_blks_dirtied  as local_blks_dirtied ,
s2.local_blks_written  - s1.local_blks_written  as local_blks_written ,
s2.temp_blks_read      - s1.temp_blks_read      as temp_blks_read     ,
s2.temp_blks_written   - s1.temp_blks_written   as temp_blks_written  ,
s2.blk_read_time       - s1.blk_read_time       as blk_read_time      ,
s2.blk_write_time      - s1.blk_write_time      as blk_write_time     ,
s1.snapshot_id as snapshot1       ,
s2.snapshot_id as snapshot2 ,
s1.query as query
from stat_statements s1, 
     stat_statements s2
where s1.queryid = s2.queryid
and s2.calls - s1.calls > 0);
-- missing statements only in s2!!

create or replace view compare_tables as (
select         
s2.relname                ,
s2.seq_scan            - s1.seq_scan            as seq_scan            ,
s2.seq_tup_read        - s1.seq_tup_read        as seq_tup_read        ,
s2.idx_scan            - s1.idx_scan            as idx_scan            ,
s2.idx_tup_fetch       - s1.idx_tup_fetch       as idx_tup_fetch       ,
s2.n_tup_ins           - s1.n_tup_ins           as n_tup_ins           ,
s2.n_tup_upd           - s1.n_tup_upd           as n_tup_upd           ,
s2.n_tup_del           - s1.n_tup_del           as n_tup_del           ,
s2.n_tup_hot_upd       - s1.n_tup_hot_upd       as n_tup_hot_upd       ,
s2.n_live_tup          - s1.n_live_tup          as n_live_tup          ,
s2.n_dead_tup          - s1.n_dead_tup          as n_dead_tup          ,
s2.n_mod_since_analyze - s1.n_mod_since_analyze as n_mod_since_analyze ,
s2.last_vacuum         ,
s2.last_autovacuum     ,
s2.last_analyze        ,
s2.last_autoanalyze    ,
s2.vacuum_count        - s1.vacuum_count        as vacuum_count        ,
s2.autovacuum_count    - s1.autovacuum_count    as autovacuum_count    ,
s2.analyze_count       - s1.analyze_count       as analyze_count       ,
s2.autoanalyze_count   - s1.autoanalyze_count   as autoanalyze_count   ,
s1.snapshot_id as snapshot1 ,
s2.snapshot_id as snapshot2 
from stat_user_tables s1, stat_user_tables s2
where s1.relid = s2.relid);




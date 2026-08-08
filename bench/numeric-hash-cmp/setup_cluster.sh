#!/bin/bash
set -e
PGROOT=$1   # e.g. /home/user/bench/pg-base
PORT=$2
DATA=$PGROOT/data
rm -rf "$DATA"
$PGROOT/bin/initdb -D "$DATA" -U pguser --no-sync -A trust > /dev/null
cat >> "$DATA/postgresql.conf" <<CONF
port = $PORT
listen_addresses = ''
unix_socket_directories = '$PGROOT'
shared_buffers = 1GB
work_mem = 512MB
hash_mem_multiplier = 8.0
maintenance_work_mem = 512MB
max_parallel_workers_per_gather = 0
max_parallel_maintenance_workers = 0
jit = off
autovacuum = off
fsync = off
full_page_writes = off
synchronous_commit = off
checkpoint_timeout = '60min'
max_wal_size = '16GB'
track_io_timing = off
log_min_messages = warning
CONF
$PGROOT/bin/pg_ctl -D "$DATA" -l $PGROOT/server.log -w start > /dev/null
echo "started $PGROOT on $PORT"

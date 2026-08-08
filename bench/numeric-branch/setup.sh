#!/bin/bash
set -e
ROOT=/home/user/bench2
rm -rf $ROOT/data $ROOT/sock; mkdir -p $ROOT/sock
$ROOT/pg/v0/bin/initdb -D $ROOT/data -U pguser --no-sync -A trust > /dev/null
cat >> $ROOT/data/postgresql.conf <<CONF
listen_addresses = ''
unix_socket_directories = '$ROOT/sock'
shared_buffers = 2GB
work_mem = 512MB
hash_mem_multiplier = 4.0
maintenance_work_mem = 512MB
max_worker_processes = 8
max_parallel_workers = 8
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
echo "cluster initialised"

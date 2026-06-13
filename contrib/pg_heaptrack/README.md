# pg_heaptrack

Profile a running PostgreSQL backend's memory-context (`palloc`/`pfree`) traffic
with [heaptrack](https://github.com/KDE/heaptrack), at **chunk** granularity, on
demand from SQL — no `LD_PRELOAD`, no debugger attach.

## How it works

A server built with `-DUSE_HEAPTRACK` (see `src/include/pg_config_manual.h`) maps
the Valgrind mempool client requests in `mcxt.c`/`alignedalloc.c` and the
context-reset chunk walkers in the allocators onto heaptrack's recording engine
(see `src/include/utils/memdebug.h`). Unlike Valgrind's client requests, which
are inert instructions, heaptrack needs real recording code in the process, so
the **engine is compiled directly into the backend**. Every report is gated on
the core-owned switch `pg_heaptrack_active` (false by default), and the engine
stays dormant until `heaptrack_init()` runs — so an un-profiled backend pays
only one predicted-false branch per allocation and no chunk walk on context
reset.

This module is the activation surface. It calls `heaptrack_init()` once and
flips `pg_heaptrack_active` around the region to profile (`heaptrack_pause()`/
`resume()` for later toggling). Because it uses `heaptrack_init()` rather than
heaptrack's preload/inject malloc interposers, only the chunk allocations are
recorded — the underlying malloc'd blocks are not, so there is **no double
counting**.

## Requirements

- A server built with `-DUSE_HEAPTRACK` (the backend compiles heaptrack's engine
  in — see "Building"). Building the module against a server not built this way
  fails at link time with an unresolved symbol — there is no run-time platform
  check.
- Linux or FreeBSD: heaptrack's engine compiles only there, so `USE_HEAPTRACK`
  (and hence this module) can only be built on those platforms.

## Building

This tree has no heaptrack source checkout, so point `HEAPTRACK_SRC` at a
heaptrack clone (e.g. `~/pg/contrib/heaptrack/src`). The backend's Makefile
compiles heaptrack's engine sources directly into `postgres` when
`-DUSE_HEAPTRACK` is set (the two CMake-generated headers it needs are
substituted from `contrib/pg_heaptrack/engine/include`), so there is no separate
archive to build.

1. Build the server with `USE_HEAPTRACK`, telling it where heaptrack's sources
   are. `--enable-cassert` makes the reset walkers report exactly the live chunk
   set:

   ```sh
   ./configure --enable-cassert ... CPPFLAGS='-DUSE_HEAPTRACK'
   make HEAPTRACK_SRC=$HOME/pg/contrib/heaptrack/src && make install
   ```

   The backend links the engine (`libheaptrack.cpp` + the unwind-tables trace
   backend, engine-only — no malloc interposers, hence no double counting) plus
   `-lstdc++ -lpthread -lrt -ldl`. `configure` already puts `-Wl,--export-dynamic`
   in `LDFLAGS_EX_BE`, which exports the engine's `heaptrack_init`/`stop`/`pause`/
   `resume` and `pg_heaptrack_active` so this module can resolve them at load
   time. (See the `USE_HEAPTRACK` block in `src/backend/Makefile`.)

2. Build and install the module (it includes heaptrack's `libheaptrack.h`, so
   point `HEAPTRACK_SRC` at the same clone):

   ```sh
   make -C contrib/pg_heaptrack HEAPTRACK_SRC=$HOME/pg/contrib/heaptrack/src install
   ```

## Usage

Profile a single session:

```sql
LOAD 'pg_heaptrack';            -- recording starts immediately
-- run the queries you want to profile
SELECT pg_heaptrack_stop();     -- optional: pause now (flush still happens at exit)
SELECT pg_heaptrack_start();    -- resume
SELECT pg_heaptrack_is_active();
```

Profile every backend (one data file per PID):

```
session_preload_libraries = 'pg_heaptrack'
```

Do **not** use `shared_preload_libraries`: that would initialise the engine in
the postmaster before any backend forks. The module refuses it.

## Analysing the result

Each backend writes one file into the data directory,
`$PGDATA/heaptrack.postgres.<pid>`:

```sh
heaptrack_interpret < "$PGDATA/heaptrack.postgres.12345" > out.interp
heaptrack_gui out.interp        # or heaptrack_print
```

## Limitations

- Allocations made before profiling starts are unknown to heaptrack; frees of
  them are ignored, and "leaked" means "allocated since start and not yet
  freed".
- `palloc_aligned()` chunks are reported under their aligned pointer and show as
  leaked after a context reset (a `-DUSE_HEAPTRACK` limitation, not specific to
  this module).
- Recording carries heaptrack's usual cost: a global lock and a stack unwind per
  allocation, with palloc traffic far exceeding malloc traffic. Profile focused
  workloads, not full benchmark runs. Do not enable `USE_HEAPTRACK` in
  production.
```

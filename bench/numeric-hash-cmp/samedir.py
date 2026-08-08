"""Same-datadir A/B: both binaries are started against ONE data directory in
turn, so data layout, page cache and file locations are identical and the only
difference between measurements is the executable."""
import subprocess, re, statistics, sys, json, os
DATA = "/home/user/bench/pg-base/data"
SOCK = "/home/user/bench/pg-base"
PORT = 5440
BINS = {"base": "/home/user/bench/pg-base/bin", "head": "/home/user/bench/pg-head/bin"}
TIME_RE = re.compile(r"^Time: ([0-9.]+) ms", re.M)

Q = [
 ("A2  1k groups (positive control)", "SELECT count(*) FROM (SELECT g_1k FROM bench GROUP BY g_1k) s;"),
 ("A5  5M groups, all unique",        "SELECT count(*) FROM (SELECT g_uniq FROM bench GROUP BY g_uniq) s;"),
 ("B4  sum(v), 5M unique groups",     "SELECT count(*) FROM (SELECT g_uniq, sum(v) FROM bench GROUP BY g_uniq) s;"),
 ("C4  200-digit numeric (fallback)", "SELECT count(*) FROM (SELECT g_200 FROM bench_long GROUP BY g_200) s;"),
 ("C5  toasted numeric",              "SELECT count(*) FROM (SELECT g_big FROM bench_toast GROUP BY g_big) s;"),
 ("C6  int8 control (no numeric)",    "SELECT count(*) FROM (SELECT g_1k_int FROM bench GROUP BY g_1k_int) s;"),
 ("D2  DISTINCT, 5M unique",          "SELECT count(*) FROM (SELECT DISTINCT g_uniq FROM bench) s;"),
 ("D4  ORDER BY numeric, 5M rows",    "SELECT g_uniq FROM bench ORDER BY g_uniq OFFSET 4999999;"),
 ("D5  filter = (numeric_eq)",        "SELECT count(*) FROM bench WHERE g_uniq = 12345.67;"),
]

def sh(cmd):
    return subprocess.run(["su", "pguser", "-c", cmd], capture_output=True, text=True)

def start(build):
    sh("%s/pg_ctl -D %s -o '-p %d' -l /home/user/bench/samedir.log -w start" % (BINS[build], DATA, PORT))
def stop(build):
    sh("%s/pg_ctl -D %s -m fast -w stop" % (BINS[build], DATA))

def run(build, sql):
    p = sh("%s/psql -h %s -p %d -U pguser -d postgres -X -q -v ON_ERROR_STOP=1 -f -" % (BINS[build], SOCK, PORT))
    return p

def timed(build, sql):
    root = BINS[build]
    p = subprocess.run(["su","pguser","-c","%s/psql -h %s -p %d -U pguser -d postgres -X -q -v ON_ERROR_STOP=1 -f -" % (root, SOCK, PORT)],
        input="\\timing on\n\\o /dev/null\n%s\n" % sql, capture_output=True, text=True)
    if p.returncode != 0: raise RuntimeError(p.stderr[:300])
    return float(TIME_RE.findall(p.stdout)[-1])

res = {n: {"base": [], "head": []} for n, _ in Q}
SESSIONS = 6           # alternating base/head server sessions
REPS = 5               # timed repetitions per query per session

# whichever server is currently up must be shut down first
for b in BINS: stop(b)

for s in range(SESSIONS):
    build = "base" if s % 2 == 0 else "head"
    start(build)
    for name, sql in Q:              # warm-up pass
        timed(build, sql)
    for r in range(REPS):
        for name, sql in Q:
            res[name][build].append(timed(build, sql))
    stop(build)
    print("session %d/%d (%s) done" % (s + 1, SESSIONS, build), flush=True)

json.dump(res, open("/home/user/bench/run/results_samedir.json","w"), indent=1)
hdr = "%-36s %9s %9s %9s %9s %8s %8s" % ("query","base med","head med","base min","head min","dmed%","dmin%")
print(); print(hdr); print("-"*len(hdr))
for name, _ in Q:
    d = res[name]
    bm, hm = statistics.median(d["base"]), statistics.median(d["head"])
    bn, hn = min(d["base"]), min(d["head"])
    print("%-36s %9.1f %9.1f %9.1f %9.1f %+8.1f %+8.1f" % (name, bm, hm, bn, hn, (hm-bm)/bm*100, (hn-bn)/bn*100))

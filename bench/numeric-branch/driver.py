"""Five-point same-data-directory benchmark.

One data directory is served by each build in turn, so data layout, file
placement and page cache are identical across builds and the executable is
the only thing that differs.  ASLR is disabled for the postmaster so the
restarts between sessions do not re-roll memory layout.  Build order is
rotated and mirrored across sessions so linear drift cancels.

v0b is an independent rebuild of the same commit as v0: the v0b-vs-v0
column is the noise floor of this whole design.
"""
import subprocess, re, statistics, sys, json, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from queries import QUERIES

ROOT  = "/home/user/bench2"
DATA  = ROOT + "/data"
SOCK  = ROOT + "/sock"
PORT  = 5450
BUILDS = ["v0", "v0b", "v1", "v2", "v3"]
COMMIT = {"v0": "fd2b898 base", "v0b": "fd2b898 rebuild (A/A)", "v1": "9035685 int128 sum",
          "v2": "693023f serialization", "v3": "c3b903a hash/compare"}
# each build twice, second half mirrored so drift cancels
SESSIONS = ["v0", "v1", "v2", "v3", "v0b", "v0b", "v3", "v2", "v1", "v0"]
REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
TIME_RE = re.compile(r"^Time: ([0-9.]+) ms", re.M)

def bin(b):  return "%s/pg/%s/bin" % (ROOT, b)

def sh(cmd, inp=None):
    return subprocess.run(["su", "pguser", "-c", cmd], input=inp,
                          capture_output=True, text=True)

def start(b):
    r = sh("setarch -R %s/pg_ctl -D %s -o '-p %d' -l %s/server.log -w start" % (bin(b), DATA, PORT, ROOT))
    if r.returncode != 0:
        raise RuntimeError("start %s failed: %s %s" % (b, r.stdout[-400:], r.stderr[-400:]))

def stop(b):
    sh("%s/pg_ctl -D %s -m fast -w stop" % (bin(b), DATA))

def timed(b, setup, sql):
    script = "%s\n\\timing on\n\\o /dev/null\n%s\n" % (setup, sql)
    r = sh("%s/psql -h %s -p %d -U pguser -d postgres -X -q -v ON_ERROR_STOP=1 -f -" % (bin(b), SOCK, PORT), script)
    if r.returncode != 0:
        raise RuntimeError("%s: %s" % (b, r.stderr.strip()[:400]))
    m = TIME_RE.findall(r.stdout)
    if not m:
        raise RuntimeError("no timing for %s: %r" % (b, r.stdout[:200]))
    return float(m[-1])

res = {q[0]: {b: [] for b in BUILDS} for q in QUERIES}

for b in BUILDS:            # make sure nothing is left running
    stop(b)

for n, b in enumerate(SESSIONS):
    start(b)
    for q in QUERIES:       # warm-up pass, untimed
        timed(b, q[3], q[4])
    for r in range(REPS):
        for q in QUERIES:
            res[q[0]][b].append(timed(b, q[3], q[4]))
    stop(b)
    print("session %2d/%d  %-4s done" % (n + 1, len(SESSIONS), b), flush=True)

json.dump(res, open(ROOT + "/run/results.json", "w"), indent=1)

def med(v): return statistics.median(v)

print()
print("v0 = fd2b898 (upstream base).  Each later column is the delta against v0.")
print("v0b is an independent rebuild of v0: its column is this design's noise floor.\n")
hdr = ("%-6s %-52s %9s %7s %7s %7s %7s %7s" %
       ("id", "case", "v0 ms", "v0b%", "v1%", "v2%", "v3%", "rsd%"))
print(hdr); print("-" * len(hdr))
rows = []
for qid, label, target, setup, sql in QUERIES:
    d = res[qid]
    b0 = med(d["v0"])
    cells = [(med(d[b]) - b0) / b0 * 100.0 for b in ("v0b", "v1", "v2", "v3")]
    rsd = max(statistics.pstdev(d[b]) / med(d[b]) * 100 for b in BUILDS)
    print("%-6s %-52s %9.1f %+7.1f %+7.1f %+7.1f %+7.1f %7.1f" %
          (qid, label[:52], b0, cells[0], cells[1], cells[2], cells[3], rsd))
    rows.append(dict(id=qid, label=label, target=target, v0=b0,
                     v0b=med(d["v0b"]), v1=med(d["v1"]), v2=med(d["v2"]), v3=med(d["v3"]),
                     d_v0b=cells[0], d_v1=cells[1], d_v2=cells[2], d_v3=cells[3], rsd=rsd))
json.dump(rows, open(ROOT + "/run/summary.json", "w"), indent=1)

import subprocess, re, statistics, sys, json, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from queries import QUERIES

BUILDS = {
  "base": ("/home/user/bench/pg-base", 5440),
  "head": ("/home/user/bench/pg-head", 5441),
}
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
TIME_RE = re.compile(r"^Time: ([0-9.]+) ms", re.M)

def run(build, setup, sql):
    root, port = BUILDS[build]
    script = "%s\n\\timing on\n\\o /dev/null\n%s\n" % (setup, sql)
    p = subprocess.run(
        ["su", "pguser", "-c",
         "%s/bin/psql -h %s -p %d -U pguser -d postgres -X -q -v ON_ERROR_STOP=1 -f -" % (root, root, port)],
        input=script, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError("%s: %s" % (build, p.stderr.strip()[:400]))
    m = TIME_RE.findall(p.stdout)
    if not m:
        raise RuntimeError("no timing: %r %r" % (p.stdout[:300], p.stderr[:300]))
    return float(m[-1])

results = {name: {"base": [], "head": []} for name, _, _ in QUERIES}

# warm-up: two untimed passes so pages are in shared buffers / page cache
for rep in range(2):
    for name, setup, sql in QUERIES:
        for b in ("base", "head"):
            run(b, setup, sql)
    print("warmup pass %d done" % (rep + 1), flush=True)

for r in range(ROUNDS):
    # alternate which build goes first, so any drift is shared
    order = ("base", "head") if r % 2 == 0 else ("head", "base")
    for name, setup, sql in QUERIES:
        for b in order:
            results[name][b].append(run(b, setup, sql))
    print("round %d/%d done" % (r + 1, ROUNDS), flush=True)

json.dump(results, open("/home/user/bench/run/results.json", "w"), indent=1)

def fmt(name, d):
    bm, hm = statistics.median(d["base"]), statistics.median(d["head"])
    bmin, hmin = min(d["base"]), min(d["head"])
    # paired per-round speedup, robust to drift
    pairs = [b / h for b, h in zip(d["base"], d["head"])]
    return (name, bm, hm, (hm - bm) / bm * 100.0, bmin, hmin,
            (hmin - bmin) / bmin * 100.0, statistics.median(pairs),
            statistics.pstdev(d["base"]) / bm * 100, statistics.pstdev(d["head"]) / hm * 100)

print()
hdr = "%-56s %9s %9s %8s %9s %9s %8s %7s %6s" % (
    "query", "base med", "head med", "delta%", "base min", "head min", "dmin%", "x", "rsd%")
print(hdr); print("-" * len(hdr))
for name, _, _ in QUERIES:
    n, bm, hm, d, bmi, hmi, dmi, sp, rb, rh = fmt(name, results[name])
    print("%-56s %9.1f %9.1f %+8.1f %9.1f %9.1f %+8.1f %7.3f %6.1f" %
          (n, bm, hm, d, bmi, hmi, dmi, sp, max(rb, rh)))

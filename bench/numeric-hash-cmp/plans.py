import subprocess, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from queries import QUERIES
BUILDS = {"base": ("/home/user/bench/pg-base", 5440), "head": ("/home/user/bench/pg-head", 5441)}
for name, setup, sql in QUERIES:
    out = {}
    for b, (root, port) in BUILDS.items():
        script = "%s\nEXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, COSTS OFF, SUMMARY OFF) %s\n" % (setup, sql)
        p = subprocess.run(["su","pguser","-c","%s/bin/psql -h %s -p %d -U pguser -d postgres -X -qAt -v ON_ERROR_STOP=1 -f -" % (root, root, port)],
                           input=script, capture_output=True, text=True)
        out[b] = p.stdout.strip() if p.returncode == 0 else "ERR " + p.stderr[:200]
    same = "SAME" if [l.split("Memory Usage")[0] for l in out["base"].splitlines()] == [l.split("Memory Usage")[0] for l in out["head"].splitlines()] else "DIFF"
    top = " | ".join(l.strip() for l in out["base"].splitlines()[:3])
    disk = "DISK-SPILL" if "Disk Usage" in out["base"] + out["head"] else ""
    print("%-56s %-4s %s %s" % (name, same, disk, top[:150]))

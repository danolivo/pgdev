"""Production-flag benchmark: 4 points, one shared data directory.

Rig invariants (see PLAN.md):
  * each build serves the SAME data directory in turn -> layout/page-cache fixed
  * ASLR off for the postmaster -> restarts don't re-roll code layout
  * session order rotated then mirrored -> linear drift cancels
  * reps back-to-back in one psql session -> constant cache state per sample
  * minima, not medians -> one-sided interference spikes are rejected
  * a cache warm-up pass before the first timed session
"""
import subprocess, re, statistics, sys, json, os
sys.path.insert(0, "/home/user/prod/run")
from queries_embed import QUERIES

ROOT="/home/user/prod"; DATA="/home/user/bench2/data"; SOCK="/home/user/bench2/sock"; PORT=5451
BUILDS=["p0","p0b","pe"]
SESSIONS=["p0","p0b","pe","pe","p0b","p0"]
WARM=2; REPS=5
TIME_RE=re.compile(r"^Time: ([0-9.]+) ms", re.M)
def b(t): return "%s/pg/%s/bin" % (ROOT,t)
def sh(c,i=None): return subprocess.run(["su","pguser","-c",c],input=i,capture_output=True,text=True)
def start(t):
    r=sh("setarch -R %s/pg_ctl -D %s -o '-p %d' -l %s/server.log -w start"%(b(t),DATA,PORT,ROOT))
    if r.returncode: raise RuntimeError("start %s: %s %s"%(t,r.stdout[-300:],r.stderr[-300:]))
def stop(t): sh("%s/pg_ctl -D %s -m fast -w stop"%(b(t),DATA))
def timed(t,setup,sql,n):
    script="%s\n\\timing on\n\\o /dev/null\n%s" % (setup, sql*n)
    r=sh("%s/psql -h %s -p %d -U pguser -d postgres -X -q -v ON_ERROR_STOP=1 -f -"%(b(t),SOCK,PORT),script)
    if r.returncode: raise RuntimeError("%s: %s"%(t,r.stderr.strip()[:300]))
    v=[float(x) for x in TIME_RE.findall(r.stdout)]
    if len(v)<n: raise RuntimeError("%s: got %d timings want %d"%(t,len(v),n))
    return v[-REPS:]

res={q[0]:{t:[] for t in BUILDS} for q in QUERIES}
for t in BUILDS: stop(t)

start("p0")
for qid,label,tgt,cls,setup,sql in QUERIES: timed("p0",setup,sql,2)
stop("p0"); print("cache warm-up done",flush=True)

for n,t in enumerate(SESSIONS):
    start(t)
    for qid,label,tgt,cls,setup,sql in QUERIES:
        res[qid][t].extend(timed(t,setup,sql,WARM+REPS))
    stop(t)
    print("session %d/%d  %-4s done"%(n+1,len(SESSIONS),t),flush=True)
    json.dump(res,open(ROOT+"/run/results_embed.json","w"),indent=1)
print("ALL_SESSIONS_DONE",flush=True)

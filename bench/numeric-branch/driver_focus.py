"""Focused rig for the aggregate commits.

Differences from driver.py, all aimed at resolving effects of a few percent:
  * narrow single-column tables, so the transition function dominates
  * repetitions of one query run back-to-back, so its table stays hot and
    every sample sees the same cache state
  * minima (and mean of the best three) rather than medians: the noise here
    is one-sided interference spikes, which a minimum rejects outright
"""
import subprocess, re, statistics, sys, json, os
sys.path.insert(0, "/home/user/bench2/run")
from queries_focus import QUERIES

ROOT="/home/user/bench2"; DATA=ROOT+"/data"; SOCK=ROOT+"/sock"; PORT=5450
BUILDS=["v0","v0b","v1","v2","v3"]
SESSIONS=["v0","v1","v2","v3","v0b","v0b","v3","v2","v1","v0"]
WARM=2; REPS=6
TIME_RE=re.compile(r"^Time: ([0-9.]+) ms", re.M)
def b(t): return "%s/pg/%s/bin" % (ROOT,t)
def sh(c,i=None): return subprocess.run(["su","pguser","-c",c],input=i,capture_output=True,text=True)
def start(t):
    r=sh("setarch -R %s/pg_ctl -D %s -o '-p %d' -l %s/server.log -w start"%(b(t),DATA,PORT,ROOT))
    if r.returncode: raise RuntimeError(r.stdout[-300:]+r.stderr[-300:])
def stop(t): sh("%s/pg_ctl -D %s -m fast -w stop"%(b(t),DATA))
def timed(t,setup,sql,n):
    """Run the statement n times inside ONE psql session, back to back."""
    script="%s\n\\timing on\n\\o /dev/null\n%s" % (setup, sql*n)
    r=sh("%s/psql -h %s -p %d -U pguser -d postgres -X -q -v ON_ERROR_STOP=1 -f -"%(b(t),SOCK,PORT),script)
    if r.returncode: raise RuntimeError("%s: %s"%(t,r.stderr.strip()[:300]))
    v=[float(x) for x in TIME_RE.findall(r.stdout)]
    if len(v)<n: raise RuntimeError("got %d timings, wanted %d"%(len(v),n))
    return v[-REPS:]

res={q[0]:{t:[] for t in BUILDS} for q in QUERIES}
for t in BUILDS: stop(t)
for n,t in enumerate(SESSIONS):
    start(t)
    for qid,label,setup,sql in QUERIES:
        res[qid][t].extend(timed(t,setup,sql,WARM+REPS))
    stop(t)
    print("session %2d/%d  %-4s done"%(n+1,len(SESSIONS),t),flush=True)
    json.dump(res,open(ROOT+"/run/results_focus.json","w"),indent=1)

def best3(v): return statistics.mean(sorted(v)[:3])
print("\nMinimum of %d samples per build (%d sessions x %d reps, run back-to-back)."%(
      REPS*2, 2, REPS))
print("v0 = fd2b898 upstream base.  v0b = independent rebuild of v0 = noise floor.\n")
hdr="%-5s %-50s %8s %7s %7s %7s %7s" % ("id","case","v0 min","v0b%","v1%","v2%","v3%")
print(hdr); print("-"*len(hdr))
rows=[]
for qid,label,setup,sql in QUERIES:
    d=res[qid]; m0=min(d["v0"])
    cells=[(min(d[t])-m0)/m0*100 for t in ("v0b","v1","v2","v3")]
    print("%-5s %-50s %8.1f %+7.1f %+7.1f %+7.1f %+7.1f"%(qid,label[:50],m0,*cells))
    rows.append(dict(id=qid,label=label,v0=m0,**{t:min(d[t]) for t in BUILDS},
                     d_v0b=cells[0],d_v1=cells[1],d_v2=cells[2],d_v3=cells[3],
                     b3={t:best3(d[t]) for t in BUILDS}))
json.dump(rows,open(ROOT+"/run/summary_focus.json","w"),indent=1)

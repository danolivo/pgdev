import subprocess, sys, os
sys.path.insert(0, "/home/user/prod/run")
from queries_prod import QUERIES
ROOT="/home/user/prod"; SOCK="/home/user/bench2/sock"; PORT=5451; DATA="/home/user/bench2/data"
BUILDS=["p0","p0b","p3","p4"]
def sh(c, i=None): return subprocess.run(["su","pguser","-c",c], input=i, capture_output=True, text=True)
def b(t): return "%s/pg/%s/bin" % (ROOT,t)
def start(t):
    r=sh("setarch -R %s/pg_ctl -D %s -o '-p %d' -l %s/server.log -w start" % (b(t),DATA,PORT,ROOT))
    if r.returncode: raise RuntimeError(r.stdout[-300:]+r.stderr[-300:])
def stop(t): sh("%s/pg_ctl -D %s -m fast -w stop" % (b(t),DATA))
def psql(t, script, flags="-X -q"):
    return sh("%s/psql -h %s -p %d -U pguser -d postgres %s -v ON_ERROR_STOP=1 -f -" % (b(t),SOCK,PORT,flags), script)

mode = sys.argv[1]
for t in BUILDS: stop(t)

if mode == "correctness":
    out={}
    for t in BUILDS:
        start(t)
        r = psql(t, open("/home/user/prod/sql/correctness.sql").read(), "-X -qAt")
        if r.returncode: print(t,"ERROR",r.stderr[:500])
        out[t]=[l for l in r.stdout.strip().splitlines() if "|" in l]
        stop(t)
        print("checked", t, flush=True)
    keys=[l.split("|")[0] for l in out["p0"]]
    print("\n%-18s %s" % ("property", "  ".join("%-6s"%t for t in BUILDS)))
    print("-"*70)
    allsame=True
    for idx,k in enumerate(keys):
        vals=[out[t][idx].split("|")[1] for t in BUILDS]
        same=len(set(vals))==1
        allsame &= same
        print("%-18s %s  %s" % (k, "  ".join("%-6s"%v[:6] for v in vals), "IDENTICAL" if same else "*** DIFFERS ***"))
    print("\n=> ", "all properties identical across all five builds" if allsame else "MISMATCH FOUND")

elif mode == "plans":
    res={}
    for t in ["p0","p4"]:
        start(t); res[t]={}
        for qid,label,tgt,cls,setup,sql in QUERIES:
            r = psql(t, "%s\nEXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, COSTS OFF, SUMMARY OFF) %s\n" % (setup,sql), "-X -qAt")
            res[t][qid]=r.stdout.strip() if not r.returncode else "ERR "+r.stderr[:200]
        stop(t)
    print("%-6s %-46s %-5s %-10s %s" % ("id","case","same","spill","top of plan"))
    print("-"*130)
    for qid,label,tgt,cls,setup,sql in QUERIES:
        a,c=res["p0"][qid],res["p4"][qid]
        norm=lambda s:[l.split("Memory Usage")[0].split("Batches")[0] for l in s.splitlines()]
        same="yes" if norm(a)==norm(c) else "NO"
        spill="DISK" if ("Disk Usage" in a+c or "Disk:" in a+c) else "-"
        par="par" if "Gather" in a else ""
        top=" | ".join(l.strip() for l in a.splitlines()[:3])[:70]
        print("%-6s %-46s %-5s %-10s %s %s" % (qid,label[:46],same,spill,par,top))

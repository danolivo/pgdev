#!/bin/bash
set -e
declare -A C=( [v0]=fd2b898 [v0b]=fd2b898 [v1]=9035685 [v2]=693023f [v3]=c3b903a )
mkdir -p /home/user/bench2/src /home/user/bench2/pg
for t in v0 v0b v1 v2 v3; do
  S=/home/user/bench2/src/$t
  P=/home/user/bench2/pg/$t
  [ -d "$S" ] || git -C /home/user/pgdev worktree add -f "$S" "${C[$t]}" >/dev/null 2>&1
  cd "$S"
  ./configure --prefix="$P" --without-icu --without-readline --without-zlib \
      CFLAGS="-O2 -fno-omit-frame-pointer" > /home/user/bench2/conf-$t.log 2>&1
  make -s -j4 > /home/user/bench2/make-$t.log 2>&1
  make -s install > /home/user/bench2/inst-$t.log 2>&1
  echo "BUILT $t (${C[$t]})"
done
echo ALL_BUILT

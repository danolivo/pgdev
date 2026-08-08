#!/bin/bash
set -e
# Production-typical build settings.  Differences from a bare "-O2" build that
# matter for this patch series:
#   -fstack-protector-strong : any function with a local array gets a canary.
#         numeric_unpack_local() puts a 70-byte char array on the stack of every
#         caller it inlines into (numeric_eq, hash_numeric, numeric_cmp, ...),
#         so this flag taxes exactly the functions c3b903a speeds up.
#   -D_FORTIFY_SOURCE        : instruments memcpy, which the same patch adds.
#   -fcf-protection, -fstack-clash-protection : on in RHEL/Debian packages.
#   --with-llvm              : distro builds enable JIT; needed to test jit=on.
# -march is deliberately NOT set: distro packages target generic x86-64.
PROD_CFLAGS="-O2 -g -pipe -Wall -Werror=format-security -fstack-protector-strong \
-fstack-clash-protection -fcf-protection -fasynchronous-unwind-tables \
-fno-omit-frame-pointer -mtune=generic"
PROD_CPPFLAGS="-D_FORTIFY_SOURCE=2"

build() {   # build <tag> <treepath>
  local t=$1 src=$2 P=/home/user/prod/pg/$t
  cd "$src"
  ./configure --prefix="$P" --with-llvm --without-icu --without-readline --without-zlib \
      LLVM_CONFIG=llvm-config-18 CLANG=clang-18 \
      CFLAGS="$PROD_CFLAGS" CPPFLAGS="$PROD_CPPFLAGS" > /home/user/prod/conf-$t.log 2>&1
  make -s -j4 > /home/user/prod/make-$t.log 2>&1
  make -s install > /home/user/prod/inst-$t.log 2>&1
  echo "BUILT $t"
}
mkdir -p /home/user/prod/pg /home/user/prod/src
# p0/p0b: two independent builds of the same base commit -> the A/A floor
git -C /home/user/pgdev worktree add -f /home/user/prod/src/p0  fd2b898 >/dev/null 2>&1 || true
git -C /home/user/pgdev worktree add -f /home/user/prod/src/p0b fd2b898 >/dev/null 2>&1 || true
# p3: the branch exactly as submitted
git -C /home/user/pgdev worktree add -f /home/user/prod/src/p3  c3b903a >/dev/null 2>&1 || true
# p4: the restructured v2 series (narrowed eligibility gate)
git -C /home/user/pgdev worktree add -f /home/user/prod/src/p4  numeric-int128-agg-fastpath-v2 >/dev/null 2>&1 || true
for t in p0 p0b p3 p4; do build $t /home/user/prod/src/$t; done
echo ALL_PROD_BUILT

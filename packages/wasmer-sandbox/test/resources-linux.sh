#!/bin/sh
# Root sets up an exclusive test subtree, then runs every assertion unprivileged.
# Usage: sudo sh test/resources-linux.sh /absolute/path/to/node /absolute/package/path UID GID
# Append a Node script and arguments to run integration under the same delegation.
set -eu
node_binary=$1
package_path=$2
test_uid=${3:?unprivileged UID required}
test_gid=${4:?unprivileged GID required}
shift 4
if [ "$#" = 0 ]; then set -- test/resources-linux.mjs; fi
test "$test_uid" -gt 0
test "$(id -u)" = 0
original=
while IFS=: read -r hierarchy controllers relative; do
  if [ "$hierarchy" = 0 ]; then original="/sys/fs/cgroup$relative"; break; fi
done < /proc/self/cgroup
test -n "$original"
parent="/sys/fs/cgroup/celld-resource-test-$$"
mkdir "$parent" "$parent/host" "$parent/commands"
cleanup() {
  # Only this script's dedicated subtree is touched, including on test failure.
  for group in "$parent"/commands/job-*; do
    [ -d "$group" ] || continue
    printf '1\n' > "$group/cgroup.kill"
    rmdir "$group" || true
  done
  printf '%s\n' "$$" > "$original/cgroup.procs"
  rmdir "$parent/commands" "$parent/host" "$parent"
}
trap cleanup EXIT
printf '%s\n' "$$" > "$parent/host/cgroup.procs"
printf '%s\n' '+cpu +memory +pids' > "$parent/cgroup.subtree_control"
printf '%s\n' '+cpu +memory +pids' > "$parent/commands/cgroup.subtree_control"
chown "$test_uid:$test_gid" "$parent" "$parent/cgroup.procs" \
  "$parent/commands" "$parent/commands/cgroup.procs" "$parent/commands/cgroup.subtree_control"
cd "$package_path"
export SANDBOX_TEST_CGROUP_PARENT="$parent/commands"
exec_status=0
setpriv --reuid "$test_uid" --regid "$test_gid" --clear-groups "$node_binary" "$@" || exec_status=$?
exit "$exec_status"

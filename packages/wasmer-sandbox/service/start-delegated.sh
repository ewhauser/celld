#!/bin/sh
# Run under the supplied systemd Delegate= service as the unprivileged celld UID.
set -eu
group_path=
while IFS=: read -r hierarchy controllers relative; do
  if [ "$hierarchy" = 0 ]; then group_path="/sys/fs/cgroup$relative"; break; fi
done < /proc/self/cgroup
test -n "$group_path"
mkdir -p "$group_path/supervisor" "$group_path/commands"
printf '%s\n' "$$" > "$group_path/supervisor/cgroup.procs"
printf '%s\n' '+cpu +memory +pids' > "$group_path/cgroup.subtree_control"
printf '%s\n' '+cpu +memory +pids' > "$group_path/commands/cgroup.subtree_control"
export CELLD_SANDBOX_CGROUP_PARENT="$group_path/commands"
exec /usr/bin/node /opt/celld-sandbox/service/main.mjs

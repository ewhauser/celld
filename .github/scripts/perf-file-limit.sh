#!/usr/bin/env bash
# Ten thousand resident SQLite cells need roughly 80,000 descriptors,
# before sockets and transient work. Give the harness and its children headroom.
set -euo pipefail
required=262144
hard=$(ulimit -Hn)
printf 'Performance file limits before: soft=%s hard=%s\n' "$(ulimit -Sn)" "$hard"
if [[ "$hard" != unlimited ]] && (( hard < required )); then
  if [[ "${RUNNER_ENVIRONMENT:-}" != github-hosted ]]; then
    echo "Performance scenarios need a hard open-file limit of at least $required; configure the runner's LimitNOFILE and retry." >&2
    exit 1
  fi
  # The hosted runner's inherited hard limit can be too low. Raising the
  # shell's limit lets exec and every celld child inherit it, without running
  # the workload as root or modifying the machine's persistent configuration.
  sudo prlimit --pid "$$" --nofile="$required:$required"
fi
ulimit -Sn "$required"
printf 'Performance file limits applied: soft=%s hard=%s\n' "$(ulimit -Sn)" "$(ulimit -Hn)"
exec "$@"

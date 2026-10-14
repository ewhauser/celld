# Changelog

## [0.6.2-ewhauser.4](https://github.com/ewhauser/celld/compare/v0.6.2-ewhauser.3...v0.6.2-ewhauser.4) (2026-10-14)


### Bug Fixes

* close pooled peer tunnels idle for `CELLD_PEER_TUNNEL_IDLE_MS` (default 60000) instead of reusing them, and bound each tunneled call by a reply deadline that fails as ambiguous without a replay ([#134](https://github.com/ewhauser/celld/issues/134))

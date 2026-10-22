# Changelog

## [0.6.2-ewhauser.5](https://github.com/ewhauser/celld/compare/v0.6.2-ewhauser.4...v0.6.2-ewhauser.5) (2026-10-22)


### Bug Fixes

* **perf:** distinguish load-dependent counts in nightly comparisons ([#139](https://github.com/ewhauser/celld/issues/139)) ([3eea98b](https://github.com/ewhauser/celld/commit/3eea98bb2816f14674ee6947c717c24331cd596b))

## [0.6.2-ewhauser.4](https://github.com/ewhauser/celld/compare/v0.6.2-ewhauser.3...v0.6.2-ewhauser.4) (2026-10-14)


### Bug Fixes

* close pooled peer tunnels idle for `CELLD_PEER_TUNNEL_IDLE_MS` (default 60000) instead of reusing them, and bound each tunneled call by a reply deadline that fails as ambiguous without a replay ([#134](https://github.com/ewhauser/celld/issues/134))

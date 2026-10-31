# Changelog

## [0.6.3-ewhauser.1](https://github.com/ewhauser/celld/compare/v0.6.3-ewhauser.0...v0.6.3-ewhauser.1) (2026-10-25)


### Bug Fixes

* rebase onto upstream v0.6.3 ([e9b0d45](https://github.com/ewhauser/celld/commit/e9b0d455df5f48fd6848db292605eb00a565130e))

## [0.6.2-ewhauser.5](https://github.com/ewhauser/celld/compare/v0.6.2-ewhauser.4...v0.6.2-ewhauser.5) (2026-10-25)


### Features

* Amazon MSK IAM authentication for the Kafka sink and loader ([#142](https://github.com/ewhauser/celld/issues/142)) ([56e45e3](https://github.com/ewhauser/celld/commit/56e45e38541b4ed75b5331c367dc6476d9f5674e))


### Bug Fixes

* **perf:** distinguish load-dependent counts in nightly comparisons ([#139](https://github.com/ewhauser/celld/issues/139)) ([3eea98b](https://github.com/ewhauser/celld/commit/3eea98bb2816f14674ee6947c717c24331cd596b))

## [0.6.2-ewhauser.4](https://github.com/ewhauser/celld/compare/v0.6.2-ewhauser.3...v0.6.2-ewhauser.4) (2026-10-14)


### Bug Fixes

* close pooled peer tunnels idle for `CELLD_PEER_TUNNEL_IDLE_MS` (default 60000) instead of reusing them, and bound each tunneled call by a reply deadline that fails as ambiguous without a replay ([#134](https://github.com/ewhauser/celld/issues/134))

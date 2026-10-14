---
title: Bug fixes
description: The failures addressed by the fork and the operational boundaries of each fix.
---

These fixes are included in the documented **v0.6.2-ewhauser.4** baseline.
Version references below identify the fork history; they do not assert that
later upstream releases still have each bug. See [release notes](../fork/releases/)
for rollout details.

## Recovery witnesses

### An expired lease does not prove a disk is lost

Previously, recovery could count an unreachable follower as conclusively lost
once its lease had expired for more than three lease lifetimes. A slower-starting
peer could still have acknowledged writes on its retained disk.

Since **0.5.1-ewhauser.3**, a missing address or failed seal request keeps that
member undecided. Without another complete witness or an existing
`bucket_complete` proof, recovery refuses to seal, record loss, or replace the
predecessor lease. A permanently unreachable witness can therefore prevent
startup. The fix does not repair a loss record already written by an older build.

[Implementation](https://github.com/ewhauser/celld/commit/739f2ba)

### Recovery checks the responding member

**0.5.1-ewhauser.5** added persistent disk incarnations and member identity to
seal and tail requests. A response from another member is refused before a seal
mark is written; recovery treats that member as undecided.

**0.5.1-ewhauser.7** corrected a deadlock caused by refusing all incarnation
mismatches. A replacement disk under the **same member name** now answers from
its own store and logs the superseded incarnation. An empty replacement disk
can conclusively report no fragment; if every member is conclusive and there is
no complete copy, recovery records loss.

:::caution[Node names carry disk identity]
Reusing a node name on a new disk declares the old disk lost, even if it still
exists elsewhere. Do this only when the previous disk is gone for good.
:::

[Member binding](https://github.com/ewhauser/celld/commit/f68a4cf) ·
[Replacement-disk correction](https://github.com/ewhauser/celld/commit/44e6ab4)

## Idle-follower failures

An idle leader used to ignore failed follower probes and record them as fast,
healthy append samples. Without a new write, it could keep depending on a
departed follower indefinitely.

Since **0.5.1-ewhauser.6**, three consecutive failed idle probes degrade the
shipper. Acknowledgements use bucket proofs while maintenance drains the epoch
and selects live members. A successful answer resets the count. An actual write
still degrades on its first failure; the idle tolerance does not delay it.

[Implementation](https://github.com/ewhauser/celld/commit/87e64cf)

## Aborted-actor transactions

An actor reset, including `ctx.abort()`, previously dropped the JavaScript
instance while leaving its SQLite transaction open. The next instance reused
the connection, could see uncommitted writes, and could fail subsequent
transactions with `cannot start a transaction within a transaction` until a
node restart.

The reset hook now rolls back the open transaction and discards queued puts.
The old instance cannot begin, commit, or roll back a transaction on the shared
connection. If rollback fails, storage is refused instead of exposing the
aborted writes. This fix landed after 0.5.1-ewhauser.4 and is included in
0.5.1-ewhauser.5 and the 0.6.0 fork builds.

[Issue #10](https://github.com/ewhauser/celld/issues/10) ·
[Implementation](https://github.com/ewhauser/celld/commit/818f63e)

### Retired transaction blocks

A `storage.transaction()` call whose block was retired before its callback ran
kept its place in the object's transaction queue. A retired block results from
a spent handler budget, a shutdown cancel, or a terminated isolate. Every later
transaction then waited, holding the input gate, so the object served nothing,
including plain reads, until the 30-second block limit reset it.

Since **0.6.2-ewhauser.3**, the transaction gives back its place and rejects
with `the cell event ended before its transaction could start`.

[Pull request #131](https://github.com/ewhauser/celld/pull/131)

## Silently dead peer tunnels

Peer calls reuse pooled tunnels. A stateful firewall, NAT, or proxy between
nodes can expire an idle flow and then drop its packets in both directions,
with no FIN or RST. The pooled tunnel still looked usable, so a call reused it
and waited about 15 minutes for the kernel to abandon the socket. TCP keepalive
does not help when a sidecar proxy terminates the local hop.

Since **0.6.2-ewhauser.4**, a tunnel idle for `CELLD_PEER_TUNNEL_IDLE_MS`
(default 60000) is closed instead of reused. Each call also has a reply bound:
the admission wait, the handler budget, and `CELLD_OPERATION_DEADLINE_MS` added
together. A call that reaches the bound fails as ambiguous and is not replayed,
because the owner may already have run it.

:::note[Set the idle limit below the path's timeout]
The default suits the common 300-second idle timeouts. Lower
`CELLD_PEER_TUNNEL_IDLE_MS` if any device between nodes expires idle flows
sooner.
:::

[Issue #133](https://github.com/ewhauser/celld/issues/133) ·
[Pull request #134](https://github.com/ewhauser/celld/pull/134)

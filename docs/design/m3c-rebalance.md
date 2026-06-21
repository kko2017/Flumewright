# M3c — Rebalance (dynamic partition assignment)

> **Status:** outline only — not yet designed in detail. Third of three M3 sub-milestones, and the largest.
> Detailed design is deferred; this captures the agreed direction. Built ON TOP OF M3a (and M3b).

## Where it fits
M3a uses **static** assignment: each consumer declares which partitions it reads, and non-overlap is the
consumers' responsibility. M3c makes assignment **dynamic**: the broker manages group membership and
**reassigns** partitions as members join or leave — true Kafka-style consumer groups. This is the
scalability/elasticity layer.

## The problem M3c solves
With static assignment, adding or removing a consumer means manually re-declaring partitions, and a crashed
member's partitions are not picked up by anyone. M3c lets the group **self-balance**: if a member dies, its
partitions are reassigned to the survivors; if a member joins, partitions are redistributed to share load.

## Why it's last (and its own sub-milestone)
Rebalance is the most complex and most concurrency-dangerous part of consumer groups — membership detection,
deciding a new assignment, pausing in-flight work, handing a partition (and its committed offset) from one
member to another without double-processing or gaps. Doing it after M3a+M3b means the at-least-once core
(happy + failure path) is already solid, so rebalance bugs don't get tangled with commit/redelivery bugs.

## Rough shape (to be designed properly later)
- **Membership / liveness:** the broker tracks which members are alive (likely via the poll/heartbeat that
  consumers already make — a member that stops polling is considered gone).
- **Assignment strategy:** how partitions map to members (e.g. range or round-robin across members).
  Enforces the one-partition-one-member rule dynamically.
- **Rebalance trigger:** member joins, member leaves/dies, partition count changes.
- **Handover safety:** the losing member must stop and commit (or stop cleanly) before the gaining member
  starts that partition — otherwise the partition is double-processed during the switch. This is the crux.
- **The broker becomes a group coordinator** (Kafka's term): the component that owns membership and
  assignment.

## Open questions (decide at M3c design time)
- How is liveness detected — heartbeat interval, poll timeout?
- Is rebalance "stop-the-world" for the group (simplest) or incremental/cooperative (Kafka's modern
  approach, much harder)? Phase 1 likely stop-the-world.
- How is the in-flight handover made safe (the losing member's uncommitted window)?
- Where does assignment state live, and how is it made concurrency-safe?

## Concurrency notes 🔒
This is the deepest concurrency challenge in the project — membership changes, assignment, and handover all
race against in-flight processing and commits. This is the milestone where **Microsoft Coyote** (defense
layer 5) earns its place: systematically exploring the interleavings of join/leave/commit/handover that
ordinary tests would never hit. See [concurrency-strategy](concurrency-strategy.md).

*This is a placeholder to preserve the agreed direction. The real design will be written after M3a and M3b,
when we start M3c — and it will likely need its own careful breakdown given the complexity.*

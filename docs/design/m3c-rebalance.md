# M3c — Rebalance (dynamic partition assignment)

> **Status:** designed (Phase-1 scope). Third and largest of the three M3 sub-milestones. Built ON TOP OF
> M3a (static assignment + offset commit) and M3b (redelivery + DLQ). This note records the design
> decisions; the step-by-step instruction plan lives separately (a private working document, not in the repo).
>
> **This is the first milestone that deliberately changes the broker and the proto.** M1–M3b kept the broker
> a pure log/offset substrate. M3c adds a **group coordinator** role inside the broker. Because that is the
> project's biggest scope expansion, every broker/proto change is gated explicitly (see "Scope discipline").

## Where it fits

M3a gave consumer groups **static** assignment: each consumer declares the partitions it reads, and
non-overlap ("one partition is read by at most one member") is the *consumers'* responsibility — the broker
does not enforce it. M3b added the failure path (redelivery + DLQ) on top, still over static assignment.

M3c makes assignment **dynamic**: the broker tracks group membership and **reassigns** partitions as members
join, leave, or die — true Kafka-style consumer groups. This is the elasticity/fault-tolerance layer: the
group self-balances instead of relying on a human to re-declare partitions.

## The problem M3c solves

Under static assignment two things cannot happen automatically:
- **A crashed member's partitions are orphaned.** No one picks them up; those partitions simply stop being
  consumed until a human intervenes.
- **Adding/removing a consumer means manual re-declaration.** Scaling the group is an operator action, not an
  automatic redistribution.

M3c lets the group self-balance: a dead member's partitions are reassigned to survivors; a new member
triggers redistribution to share load. The broker enforces the one-partition-one-member rule *dynamically*,
which M3a explicitly left to the consumers.

## Guiding principle — follow Kafka's proven model; simplify only the rebalance algorithm

The framing for M3c: **this is the part of the project where we adopt Kafka's battle-tested coordination
model rather than invent our own.** Membership detection (heartbeats + session timeout), the group
coordinator as a broker-side role, the broker/leader split for assignment, and generation/epoch fencing are
all mechanisms Kafka refined over many KIPs (KIP-62 separated liveness from processing; KIP-429 added
cooperative rebalancing). We do **not** simplify those — touching what Kafka proved in production is exactly
the risk we avoid.

The **one** thing we deliberately simplify is the rebalance *algorithm*: Phase 1 ships **eager
(stop-the-world)** rebalancing, which is what Kafka itself used for its first seven years (0.9–2.3) before
cooperative landed in 2.4. Eager is simpler and more predictable (fewer intermediate states), and it makes
the handover-safety problem tractable. Cooperative/incremental rebalancing is deferred to Phase 2, and the
assignment layer is shaped so that switch is graceful, not a rewrite (see Decision D).

Three layers, mirroring the M3b framing:
1. **Primitives (broker substrate)** — append-only log, offsets, subscribe, publish, commit. Complete since
   M1/M2/M3a. Unchanged by M3c.
2. **Coordination (this milestone)** — a broker-side **group coordinator** that owns membership, liveness,
   generation, and assignment distribution; plus the SDK-side group-membership client (join, heartbeat,
   receive assignment, handle revocation). This is the new work.
3. **Advanced (Phase 2 / future)** — cooperative/incremental rebalancing, static membership (stable ids to
   avoid rebalances on quick restarts), custom assignment strategies. Extensions of layer 2, not rewrites.

## Decision A — eager (stop-the-world) rebalancing, cooperative-ready

When group membership changes, the whole group briefly stops, partitions are reassigned, and the group
resumes — Kafka's original "eager" protocol:

1. The coordinator detects a membership change (join, leave, or a session-timeout death).
2. It signals a rebalance; every member stops consuming and (re-)sends a **JoinGroup** request. A member
   sending JoinGroup *is* the signal that it has stopped — this is how the coordinator knows the group has
   quiesced.
3. Once all known members have rejoined (or the rebalance timeout elapses), assignment is computed (Decision
   C) and distributed via **SyncGroup**.
4. Members receive their new assignment and resume consuming.

**Why eager for Phase 1.**
- It directly makes the **handover-safety** problem (Decision E) tractable: because *every* member stops
  before anyone resumes, there is a clean moment when no one is consuming a partition — so a partition cannot
  be processed by its old and new owner at the same time.
- It is simpler and predictable — fewer intermediate states to reason about, which matters because this is
  the project's most concurrency-dangerous milestone.
- It is the proven baseline; cooperative is an optimization on top, not a prerequisite.

**Order/progress contract.** During an eager rebalance the group's throughput drops to zero for the
rebalance window (the stop-the-world cost). This is accepted in Phase 1: correctness over availability while
the coordination core is established. Phase 2's cooperative protocol is the availability optimization.

**Cooperative-ready (the graceful-Phase-2 requirement).** Eager is implemented behind an **assignment
abstraction** (Decision D) so that moving to cooperative later changes the *strategy and the
revoke/assign rounds*, not the coordinator's membership/liveness/generation core. Concretely: the membership
table, heartbeat/session-timeout machinery, generation fencing, and the JoinGroup/SyncGroup/Heartbeat RPCs
are all reused unchanged by cooperative; only "revoke everything then reassign" becomes "revoke only what
must move." We do not pre-build cooperative, but we do not wall it off either.

## Decision B — liveness via a dedicated Heartbeat RPC (liveness separated from processing)

The coordinator must know which members are alive. We follow Kafka's model exactly, including the lesson of
KIP-62: **liveness and processing-progress are separate concerns and must not share one timeout.**

- A member sends periodic **Heartbeat** RPCs to the coordinator (a dedicated RPC, on its own cadence —
  Kafka's `heartbeat.interval.ms`), independent of the message-consumption stream.
- If the coordinator receives no heartbeat from a member within the **session timeout** (Kafka's
  `session.timeout.ms`), it declares the member dead and triggers a rebalance to reassign its partitions.
- Processing time (how long a handler takes) is a *separate* budget and does not, by itself, evict a member —
  exactly the separation KIP-62 introduced (`max.poll.interval.ms` vs `session.timeout.ms`). A slow handler
  must not be mistaken for a dead member.

**Why a dedicated RPC rather than reusing the subscribe stream.** Our consumption is a long-running
server-stream (`SubscribeGroupAsync`). If we inferred liveness from that stream's activity, a member doing
slow processing would look dead (the KIP-62 trap), and a member whose stream is healthy but whose processing
has livelocked would look alive. A separate heartbeat decouples "can this member talk to the coordinator"
from "is this member processing fast enough" — the proven Kafka separation. This is a proto addition
(`Heartbeat` RPC) and broker state (last-heartbeat per member), gated under Scope discipline.

## Decision C — broker is the coordinator; the *leader* (a consumer) computes the assignment

The coordinator is **a role inside our single broker process**, not a separate process. (Kafka runs many
brokers and one of them acts as a given group's coordinator; we have one broker, so the coordinator is one
more component beside `ITopicStore` and `ICommittedOffsetStore` — call it the group coordinator.) It owns:
membership, liveness/heartbeat tracking, the generation number, and distribution of assignments.

**But the assignment computation itself is delegated to a group *leader* — one of the consumers — exactly as
Kafka does.** On each rebalance the coordinator designates one member as leader, sends it the full member
list (and their subscriptions), the leader runs the assignment strategy and returns the result, and the
coordinator distributes it to all members via SyncGroup.

**Why follow the broker/leader split (and not just let the broker assign).** At our Phase-1 scale the broker
could compute the assignment itself, and that would be less code. We deliberately do **not** take that
shortcut, for two reasons:
- **It is the proven Kafka model, and this project is explicitly Kafka-inspired.** The split exists because
  assignment is a *policy* (range, round-robin, sticky, domain-specific) that benefits from living on the
  client where domain knowledge is, while the broker stays a neutral membership/liveness authority. Collapsing
  policy into the broker is the kind of "simplify away Kafka's design" move we agreed to avoid.
- **Separation of concerns / learning value.** The broker answers "who is alive"; the leader answers "how to
  divide." Implementing that split is a core distributed-systems lesson this milestone exists to teach.

The cost is honestly recorded: the split adds RPCs and a leader-selection step (more proto surface, more
states). That is acceptable and is exactly why Scope discipline (every broker/proto change gated) matters here.

**Leader selection and leader failure (designed, not left to implementation).** Following Kafka, leader
selection is deliberately trivial: **the first member to send JoinGroup in a rebalance is the leader** for
that generation — deterministic, no election protocol, no tie-break. The leader is *not* a privileged
long-lived role; it is re-picked every rebalance, so leadership is per-generation. This makes the failure
cases collapse into the normal rebalance path rather than needing special handling:
- **Leader dies before returning an assignment** → it stops heartbeating → session timeout (or the rebalance
  timeout, whichever fires first) → the coordinator bumps generation and starts a fresh rebalance, in which a
  new first-joiner becomes leader. No "leader failover" code exists; leader death is just a membership change.
- **Leader returns an assignment but a follower died meanwhile** → the dead follower misses SyncGroup / its
  heartbeat lapses → another rebalance. Same path.
- **Leader is slow to compute** → bounded by the rebalance timeout; if it misses, it is treated as gone and
  the rebalance restarts.

The single rule "every failure during a rebalance just triggers another rebalance at a higher generation"
keeps the state machine small — there is no separate recovery mode, only the generation-bump loop.

## Decision D — assignment behind a strategy interface (the cooperative-ready seam)

Assignment is **policy-driven**, not hard-coded — the same "open the structure, defer the implementation"
move M3b used for `RetryPolicy`. An assignment strategy maps `(members, their subscriptions, partitions) →
per-member partition assignment`. Phase 1 ships one strategy (range or round-robin across members, enforcing
one-partition-one-member); the interface is shaped so:
- additional strategies (sticky, cooperative-sticky) are added later without changing the coordinator, and
- the eager→cooperative move in Phase 2 is a change in the *revoke/assign rounds* and the strategy, not in the
  membership/liveness/generation core.

This is the concrete form of the "cooperative-ready" requirement from Decision A: the seam is the strategy
interface plus a clean separation between "decide assignment" (leader, swappable) and "detect membership /
fence generations / distribute" (coordinator, stable).

## Decision E — handover safety (the crux): stop-the-world + generation fencing together

The central correctness problem of any rebalance: when a partition moves from member A to member B, B must
not start consuming it until A has stopped and committed. If their windows overlap, the same offsets are
processed by both — double-processing that idempotency cannot always paper over (it corrupts *progress*, not
just produces a duplicate). 14 called this "the crux"; it is.

Two mechanisms together close it:

**1. Eager stop-the-world gives a clean quiescent moment.** Because every member stops and re-sends JoinGroup
before *anyone* receives a new assignment, there is a point where no member is consuming any partition. B
receives P only after that point, so in the normal (cooperative-member) case there is no overlap by
construction. This is the main reason eager was chosen (Decision A).

**2. Generation fencing handles the member that did NOT stop cleanly (the zombie).** Stop-the-world assumes
members cooperate. But a member can be declared dead by session timeout while it is actually just slow or
network-partitioned — then it comes back and tries to commit or consume against partitions it no longer owns.
Without protection, that stale commit can overwrite the new owner's progress and reopen the crux. Kafka's
answer, which we adopt:

- The coordinator holds a **generation** number for the group, incremented on every rebalance.
- Every member learns its generation at JoinGroup/SyncGroup time and stamps it on **commit** and
  **heartbeat** requests.
- The broker **rejects any commit or heartbeat carrying a stale generation** (`generation < current`) with a
  "fenced / rebalance in progress — rejoin" error. The zombie's late commit is refused; it is forced to
  rejoin as a fresh member of the current generation.

Eager alone is not enough (it assumes cooperation); generation fencing alone is not enough (it does not give
the quiescent moment). Together they make handover safe: the clean stop covers the common case, fencing
covers the straggler/zombie case.

**Where the fence check lives — and the regression caution.** The commit-side fence sits in the broker's
commit path — the *same* critical section that M3a's `CommitOffsetAsync` already uses for range/backwards
validation (the code FIX-011 hardened). Adding a generation check there means **modifying M3a-merged code**,
so this is a high-risk, checkpoint-gated change, and the M3a commit/resume integration tests must be re-run
to prove no regression (the FIX-014 lesson: merged code can still hide behavioural defects until a test
exercises it). The generation check is a single integer comparison inside the existing lock — negligible at
runtime; the cost is implementation risk, not performance.

## Decision F — generation/epoch as the consistency token

Generation is the consistency token threaded through the whole protocol, not just the commit fence:

- **Bumped** by the coordinator on every rebalance (join, leave, session-timeout death).
- **Carried** in SyncGroup (member learns current generation), Heartbeat (coordinator can tell a member "you
  are stale, rejoin"), and CommitOffset (the fence above).
- **Checked** wherever a member's action must be valid only for the assignment it was given: a commit or
  heartbeat from generation *g* is honored only while the group is still at *g*.

This mirrors Kafka's generation/epoch exactly and is the standard zombie-fencing pattern. It is the reason
the broker — not the consumers — must hold membership state: only a central authority can issue a
monotonic generation and reject the stale.

Phase-1 scope note: we add the generation *mechanism* (bump, carry, fence). We do **not** add Kafka's static
membership (`group.instance.id`, stable ids that survive quick restarts to avoid a rebalance) — that is a
Phase-2 optimization on top of this same generation core.

## Mechanism (Phase-1 M3c) — the eager rebalance lifecycle

A single rebalance, end to end:

1. **Trigger.** The coordinator observes a membership change: a new member's first JoinGroup, an explicit
   LeaveGroup, or a session-timeout expiry (no heartbeat within `session.timeout.ms`).
2. **Bump generation.** The coordinator increments the group generation. All in-flight commits/heartbeats at
   the old generation will now be fenced.
3. **Stop-the-world / rejoin.** The coordinator marks the group "rebalancing". Members, on their next
   heartbeat, are told to rejoin; each stops consuming and sends JoinGroup. The coordinator waits for all
   known members to rejoin, bounded by a **rebalance timeout** (a member that misses it is dropped from the
   new generation).
4. **Leader computes assignment.** The coordinator picks one member as leader, sends it the member list +
   subscriptions, and the leader runs the assignment strategy (Decision D) → a per-member partition map.
5. **Distribute via SyncGroup.** The leader returns the assignment to the coordinator; the coordinator sends
   each member its slice (and the current generation) in the SyncGroup response.
6. **Resume.** Each member begins consuming its assigned partitions from each partition's committed offset
   (M3a semantics: committed = next offset to read, DEC-023). The group is "stable" again.

Throughout, **commit and heartbeat carry the generation**; anything stale is fenced (Decision E/F).

**Relationship to M3a static assignment — coexist, but mutually exclusive (the Kafka rule).** M3a's "consumer
declares its partitions" path is **retained**, as the equivalent of Kafka's `assign()` (manual) vs
`subscribe()` (group-managed). But coexistence is **not** "use both freely" — Kafka makes them *mutually
exclusive on a single consumer* (calling both throws "Subscription to topics, partitions and pattern are
mutually exclusive"), and for good reason. We adopt the same rule, and extend the caution to the group level:

- **Per consumer:** a consumer uses *either* the M3a static API *or* the M3c dynamic join/heartbeat/sync flow,
  never both. The two are exclusive.
- **Per group (the real hazard):** a static (manually-assigned) member does not participate in the coordinator's
  membership/generation tracking — it never joins, never heartbeats, carries no generation. If such a member
  reads/commits the *same* partitions a dynamic member is being assigned, the coordinator cannot see it and
  **generation fencing (Decision E/F) cannot protect against it** — the static member is an unfenced actor on a
  partition the group thinks it owns. That reopens the crux. So static and dynamic membership must not target
  overlapping partitions of the same group.

The design therefore keeps both APIs (M3a/M3b behavior and tests stay intact, so M3c is additive) but treats
mixing them within a group on the same partitions as a usage error, mirroring Kafka. Whether the broker should
actively *reject* an overlap (vs document it as a contract) is an open point for design review — rejecting is
safer but adds broker bookkeeping about which members are static.

## Scope discipline — every broker/proto change is gated

M3c is the first milestone to change the broker and the proto, so the M1–M3b "broker is immutable" hard rule
is replaced by an **explicit, gated** list. The expected proto/broker surface:

- **proto:** `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` RPCs; a `generation` field on the relevant
  requests/responses and on `CommitOffset`.
- **broker:** a new `IGroupCoordinator` component (membership table, per-member last-heartbeat, generation,
  group state machine: stable / rebalancing); a generation check added to the existing `CommitOffsetAsync`
  critical section.
- **core SDK:** group-membership client (background heartbeat loop, join/sync, applying the received
  assignment, handling "rejoin" signals and partition revocation).

Each of these is introduced in its own step and reviewed at a checkpoint. Anything **not** on this list — new
broker responsibilities beyond coordination — is still out of bounds: if a step seems to need it, STOP and
ask. The point of the gating is that "the broker may change now" does not become "the broker may change
freely."

## Concurrency notes 🔒 — the deepest surface in the project

This is the milestone 11's defense-in-depth was built for, and where **Microsoft Coyote (Layer 5)** finally
earns its place. The hazards, all new:

- **Membership table is shared mutable state** touched concurrently by: heartbeat arrivals, join/leave
  requests, the session-timeout sweeper, and commit-path generation checks. It needs its own lock discipline,
  separate from the topic/offset stores.
- **The session-timeout sweeper races membership changes** — a member can heartbeat at the same instant the
  sweeper decides it is dead. The "is it alive" decision and the "evict + bump generation" action must be one
  atomic step (the same check-then-act discipline as FIX-011), or a live member gets evicted / a dead one
  lingers.
- **Generation bump vs in-flight commit** — the fence is exactly the race we are defending: a commit must be
  validated against the generation *atomically* with the read of the current generation, inside the lock, or a
  commit can slip across a rebalance boundary.
- **Group state machine transitions** (stable ↔ rebalancing) must be serialized; two triggers (a join and a
  death) arriving together must not start two overlapping rebalances.

These are precisely the join/leave/commit/handover interleavings ordinary tests hit only by luck — the
argument for adding Coyote here. The defense layers from 11 all apply, and the non-atomic-boundary +
shared-lifetime-task disciplines (FIX-014/015) carry over to the coordinator and the SDK heartbeat loop.

## Testing approach

End-to-end over a real broker (Kestrel + gRPC), the M3a/M3b integration-test pattern, plus — for the first
time — systematic interleaving exploration:

- **Membership lifecycle:** a member joins → gets an assignment; a second joins → partitions redistribute; a
  member leaves/dies (stops heartbeating) → its partitions reassign to survivors.
- **Handover safety (the crux):** assert no double-processing across a rebalance — the gaining member starts
  only from the committed offset, and a *zombie* (a member that resumes after being declared dead) has its
  stale-generation commit **rejected** (the fence works).
- **Liveness separation (KIP-62):** a member with slow processing but healthy heartbeats is **not** evicted;
  a member that stops heartbeating **is** — proving liveness ≠ processing time.
- **No regression in M3a/M3b:** the existing commit/resume and redelivery/DLQ suites still pass with the
  generation field threaded through (the FIX-014 lesson — re-run the merged-code tests).
- **Coyote (Layer 5):** systematic exploration of join/leave/commit/handover interleavings for the
  coordinator's critical sections. (Introduced this milestone.) **Scope clarification:** Coyote's binary
  rewriting instruments whole assemblies, but what it actually *explores* is only the code a given concurrency
  unit test drives — so adding Coyote does **not** mean re-testing the entire project. We write new concurrency
  unit tests that exercise the `IGroupCoordinator` critical sections directly, **in-process, not through gRPC**
  (Coyote controls in-process Task scheduling; it cannot follow concurrency across a network/gRPC boundary, and
  would error on un-rewritten external concurrency). So the layering is: existing xUnit unit tests unchanged;
  gRPC-based integration tests for end-to-end behavior; and Coyote concurrency unit tests aimed squarely at the
  coordinator component. The set of assemblies to rewrite (coordinator + its dependencies, not the gRPC/Kestrel
  host) and the exact critical sections to model are set at design review.

Bounded waits (FIX-008), no fake-green (FIX-012), no `Task.Delay`-as-sync, exact assertions — same disciplines
as M3a/M3b.

## Deferred to Phase 2 (opened, not built)

- **Cooperative / incremental rebalancing** (KIP-429): revoke only what must move, no stop-the-world. The
  assignment strategy interface (Decision D) and the membership/generation core are shaped to accept it
  without a rewrite.
- **Static membership** (`group.instance.id`): stable ids so a quick restart does not trigger a rebalance.
  Builds on the same generation core.
- **Custom/sticky assignment strategies:** more strategies behind the Decision-D interface.
- **Incremental/cooperative handover** of in-flight work: only relevant once cooperative lands.

## Out of scope for M3c

- **Multi-broker coordination.** We have one broker; the coordinator is a role inside it. Distributing the
  coordinator across brokers (Kafka's coordinator-per-group hashing) is not in Phase 1.
- **Exactly-once / transactional consume-process-commit.** Unchanged from M3a/M3b: at-least-once with
  consumer-side idempotency. Generation fencing prevents zombie *progress corruption*, not duplicate delivery.
- **Timed/multi-stage backoff, blocking retry** (M3b's Phase-2 items) remain Phase 2.

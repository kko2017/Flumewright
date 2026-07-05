# M4 — mTLS Design Note

> Design note for M4 step 3 (mTLS). Promoted from the private strategy draft after the two prerequisites
> landed (shift-left = DEC-028/029; e2e rebuild = FIX-022). Confirmed decisions here; anything still open is
> marked ⟳ and is finalized during implementation with a DEC entry.

## 1. What M4 is (scope)

**M4 = mutual TLS between client and broker over the existing gRPC/Kestrel transport, plus identity
extraction onto the per-request context.** Today the transport is plaintext and every client is anonymous.
After M4, both sides present CA-signed certificates and verify each other, and the broker knows *who* each
connection is — the principal rides every request.

Kafka-fidelity anchor: mirrors `ssl.client.auth=required` (client + broker both hold CA-signed certs and
verify each other). Adopted layers: (1) client⇄broker mutual auth, (2) a CA + broker/client key pairs,
(3) a "require client cert" switch, (A2) extract the identity onto the request context.

### Scope ceiling — authentication and identity only, no authorization
M4 answers "who are you?" and records the answer. It does **not** answer "may you do this?" — that is ACL,
a separate later milestone (§5). Nothing is denied based on identity in M4.

### Out of scope (fence)
- ACL / authorization enforcement (separate milestone, §5).
- Certificate rotation / lifecycle / automation.
- Inter-broker / controller mTLS (single broker; multi-broker is Phase 2).
- SASL and other auth mechanisms (SCRAM, OAuth).
- A matrix of TLS modes — M4 targets mutual auth. (The config switch in §2.3 exists for test-harness
  compatibility, not as a supported "plaintext mode" product feature.)

## 2. Architecture

### 2.1 Where the certificate check lives — Kestrel + a thin interceptor (hybrid)
- **Kestrel enforces presence and validity**: the TLS listener requires a client certificate
  (`ClientCertificateMode.RequireCertificate`) chained to our CA. An unauthenticated or untrusted client
  fails at (or immediately after) the handshake — it never reaches a handler.
- **A gRPC interceptor extracts identity**: reads the validated client certificate off the connection,
  parses the principal, and stores it on the request context. The interceptor does *not* re-do certificate
  validation (that is Kestrel's job); it only maps cert → principal.
- Rationale: validation belongs to the transport host (battle-tested code path); identity shaping belongs to
  the application layer where ACL will later consume it. This also introduces the **interceptor pipeline**
  that ACL will extend (§5).

### 2.2 Principal shape ⟳
- Direction: `User:<CN>` from the client certificate subject, mirroring Kafka's DN-as-principal convention.
- ⟳ Finalize during implementation: the exact `ServerCallContext` storage (UserState key vs HttpContext
  feature), and the exact parse (CN only vs configurable DN). Keep the stored shape stable and ACL-ready —
  a later ACL interceptor must be able to read it without knowing how it was produced. Record as a DEC.

### 2.3 The require-client-cert switch — DEC-030 configuration-seam pattern
- `Broker:` config key (e.g. `Broker:RequireClientCertificate` + cert path/password keys as needed),
  following the DEC-030 pattern: defaults preserve current behaviour, tests/environments opt in.
- **Default: off (plaintext), mTLS opt-in via config.** Consequence: the existing 21 integration tests run
  unchanged on the plaintext listener; only mTLS tests use a cert-enabled harness variant (§4.1). This is a
  test-compatibility seam, not a product "mode matrix" (§1 fence): the supported deployment story is
  mTLS-on.

### 2.4 Proto / API impact — none
mTLS lives at the transport layer, below the gRPC service contract. Identity rides the connection, not the
messages. No proto change — kept true by the §4.2 verification decision (no debug/whoami RPC).

## 3. Certificates

### 3.1 certgen tool (built in M4)
- A repo-committed **generator** (script/tool) that mints: a self-signed CA, a broker cert, and client
  certs, for local dev and CI.
- Must satisfy the plan §7.1 checklist: CA has BasicConstraints CA:true; the broker cert's SAN includes
  `localhost` (and `127.0.0.1`); client certs carry EKU clientAuth; a single CA chain signs everything.

### 3.2 Secrets discipline (hard rule)
- **No certificate or key material is ever committed** (`*.pfx` / `*.key` / `*.pem` — GEMINI.md "Never"
  list). The repo carries only the generator. Local dev and CI generate fresh certs at run time (CI: a step
  before the integration tests).

## 4. Testing strategy

### 4.1 Layer and harness
- mTLS is a transport/identity property over real gRPC → **integration tests** (real broker, real
  handshake). Not Coyote — no interleaving concern (DEC-027 tool boundary).
- **Cert-aware `BrokerAppFactory` variant** (DEC-030 seam pattern): the factory gains an opt-in mTLS mode —
  broker cert + require-client-cert on — while the default factory stays plaintext so the existing suite is
  untouched. Client side: tests inject client certs via `SocketsHttpHandler.SslOptions` on
  `GrpcChannelOptions.HttpHandler` (the same injection point `HeartbeatFault` already uses).
- Test style: stage-isolated, deterministic, per-test cts deadlines — the FIX-022 patterns.

### 4.2 What is verified where — the observability decision
M4 enforces nothing, so "the identity landed on the context" has **no externally observable effect** (no
rejection, no response change). Decision:
- **Integration tests verify the transport property**: a client with a valid CA-signed cert connects and
  RPCs succeed; a client with no / invalid / untrusted cert is rejected. (⟳ the exact observation point of
  rejection — connect-time vs first-RPC, and which `RpcException` status — is confirmed during
  implementation per DEC-031: do not assume "handshake returned" semantics; find the real observable
  moment.)
- **Unit tests verify identity extraction**: the interceptor, given a (test) certificate, produces the
  expected principal and stores it in the expected context slot.
- **The end-to-end proof that the context identity is consumed is deliberately deferred to the ACL
  milestone**, which reads it for real. This keeps M4's scope honest (the hook is laid, not exercised) and
  keeps the proto unchanged (no whoami/debug RPC added just to observe the hook).

## 5. The ACL seam (what M4 lays down for later)

- Authorization is cross-cutting → a future **authorization interceptor behind the auth interceptor**;
  handlers stay untouched. ACL is milestone-sized on its own (rule storage + evaluation engine + management
  API + enforcement on every RPC) and depends on the identity M4 establishes.
- **The hook M4 must deliver**: the principal, in a stable documented shape (§2.2), on the request context.
- **Guard for M5 (record as DEC):** throughput optimization must NOT create a fast path that bypasses the
  interceptor pipeline — or a later ACL could be bypassed with it.

## 6. Step shape (sequenced precisely in the execution instruction — `15-m4-mtls`)

Each step its own commit, checkpoints per the risk-based rules (certgen and the reject-path are
security-adjacent → checkpoint them):
1. certgen tool — built and **validated locally first** (unit tests on the produced artifacts)
2. Kestrel mTLS listener + config switch (DEC-030 pattern)
3. identity-extraction interceptor + context hook (unit-tested)
4. reject paths + cert-aware factory variant + integration tests
5. **CI cert-generation step (`ci.yml`) — deliberately separate and last**: a workflow change is a
   different risk class than product code, and the CI step can only be written once the tool it invokes
   is proven locally (steps 1–4 green).
Fewer checkpoints than M3c (smaller, less concurrency-dangerous), but not zero — three: after step 1,
after steps 2+3, after step 4.

## 7. Decisions to record when implementation starts
- DEC: ACL split / interceptor seam + the M5 no-bypass guard (§5).
- DEC: principal shape + context storage (§2.2, when finalized).
- DEC: mTLS switch default + test-harness compatibility rationale (§2.3).
- DEC: identity-verification split — unit-level now, e2e deferred to ACL (§4.2).

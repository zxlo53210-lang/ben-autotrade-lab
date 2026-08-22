# ChatGPT Pro Round 4 P0 design follow-up

## Bound design evidence

- Review label: `BEN_AUTOTRADE_PRO_ROUND4_P0_DESIGN_FOLLOWUP`
- Baseline authority commit: `7a722397f7467511ec8fd51266c83786b723de15`
- Follow-up sent at: `2026-08-22T10:23:55.025Z`
- Exact response-content SHA-256: `6b3c7607cc75900523dcf396835218fc9c8aec4aa8d56f337f01ee70e474732a`
- Exact response length: 24,044 UTF-8 bytes
- Visible model: `GPT-5.6 Sol`
- Visible capability: `Pro 5/5`
- Visible reasoning effort: `Pro`
- Exact terminal design verdict: `DESIGN_ACCEPTABLE`

This is architecture-level acceptance only. It is not `PROCEED`, does not
close the deployed P0, and does not authorize a review receipt, locked-data
access, finalization, PAPER, LIVE, credentials, accounts, orders, leverage,
borrowing, shorting, or real funds.

## Accepted minimum architecture

The P0 can be closed for the stated threat model by moving the allocation
authority outside the Windows/WSL research operator's control:

1. One globally named, independently administered Cloudflare Durable Object is
   the serial allocation point.
2. One SQLite transaction independently enforces `UNIQUE` on
   `experiment_id`, `lockbox_id`, and `holdout_commitment_sha256`, together
   with unique request, challenge, and opening-commitment identifiers.
3. Every collision, including an exact retry, returns a signed terminal
   `ALREADY_CONSUMED`; it is never an idempotent success.
4. The independent service signs a canonical, domain-separated receipt and
   submits that receipt to Rekor. It returns the complete verified inclusion
   bundle and checkpoint through the single pinned witness origin.
5. The local finalizer verifies the service signature and Rekor bundle offline
   against frozen service identity, schema, build, public key, trusted root,
   log identities, exact digest, signature, and key bindings.
6. A fresh server challenge and an independent in-memory client nonce bind a
   direct success response to the current invocation. Status responses are a
   distinct schema and always set `authorizes_locked_read = false`.
7. Local ext4 `+a`, the primary anchor, and local state remain layered defense
   in depth after the remote opening allocation is durably anchored.

The recommended opening order is:

`FROZEN -> remote atomic allocation -> service signature -> Rekor inclusion -> local +a opening burn -> primary anchor -> local HOLDOUT_OPENED -> full revalidation -> locked read`

The recommended closing order is:

`immutable report -> remote finalization -> service signature -> Rekor inclusion -> local +a finalization -> local FINALIZED`

Remote finalization must bind the remote opening receipt and Rekor bundle,
local opening burn, local opened-state hash, anchor store and record hashes,
report hash, report schema, report status and report kind. A second or changed
finalization is rejected.

## Required failure semantics

- Once the remote allocation transaction commits, all three keys are consumed
  permanently, even if signing, Rekor, networking, or local work later fails.
- An ambiguous or lost response stops the current invocation before locked
  access. Automatic POST retry is forbidden.
- A later invocation uses a fresh live challenge. If the earlier request
  committed, it receives `ALREADY_CONSUMED`; if the request never reached the
  service, a later allocation may succeed because no earlier locked read was
  authorized.
- `COMMITTED_UNSIGNED` and `SIGNED_UNANCHORED` are consumed failure states.
  Only a fresh direct `ANCHORED` response may authorize the current locked
  read.
- A lost finalization response prevents local `FINALIZED` and therefore keeps
  PAPER blocked.
- Any service, receipt, proof, schema, key, challenge, nonce, origin, build,
  state-chain, or commitment ambiguity fails closed.

## Threat boundary and remaining authorization blocker

Independent custody is the security property; Cloudflare alone is not WORM.
The research operator must have no deploy, PITR, `deleteAll`, Data Studio, DNS,
signing-key, account-recovery, or custodian-control path. If custodian or cloud
administrator rollback is also in scope, the service must synchronously retain
the complete signed receipt, Rekor bundle, checkpoint, and service hash chain
in retention-locked append-only storage under a second administrative domain,
then reconcile that immutable head before each allocation.

The current repository contract cannot implement this design: it permits only
public Binance market-data GET requests. Implementation therefore requires a
new, explicit authority decision covering:

- one exact pinned HTTPS witness origin and exact GET/POST paths;
- the independent custodian and service/store genesis identity;
- a witness-only caller capability with no exchange, account, trading, or
  general cloud authority;
- the service signing-key custody and optional second-domain WORM retention;
- transmission of hashes, IDs, authority labels, signatures, and proofs only,
  never prices, bars, returns, metrics, PnL, credentials, accounts, orders, or
  private paths.

Until those conditions are separately authorized, provisioned, implemented,
failure-injected, and independently reviewed, Round 4 remains `BLOCKED` and the
locked holdout remains unopened.

## Rejected substitutes

- Rekor alone proves inclusion but does not atomically allocate three semantic
  uniqueness keys or prove non-membership.
- OpenTimestamps alone proves existence by a later time but provides no
  immediate transaction, uniqueness, or state machine.
- GitHub under the same owner is mutable in policy and publication timing and
  is not an independent allocator.
- Cloud storage under the same operator changes location, not authority.
- A TPM NV counter is useful defense in depth but is a scalar, does not map the
  three allocation keys by itself, is machine-bound, and is not independent of
  an adversarial platform owner unless hierarchy, clear, deletion, attestation,
  and external counter-history governance are all separately solved.

The independently administered remote allocator plus Rekor is the smallest
defensible primary route for this one-host laboratory. A TPM can be added only
as a secondary control.

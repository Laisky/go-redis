# Security finding remediation

## Prefix lookup: issues #9 and #14

Baseline revision e6207a7bb48d86265b8e8ee81444ff07bf812444 reproduces literal-prefix violations for *, ?, bracket expressions and backslash. A standalone ] already has literal Redis semantics. A synthetic 1,001-key lookup completes 95 SCAN calls, reaches cursor zero and sends one 1,001-key MGET. A value over 1 MiB is fully returned. An injected read error includes matched key names.

The existing GetItemWithPrefix signature remains. It now delegates to GetItemWithPrefixLimited with conservative defaults: 1,000 distinct inspected matching keys, 1 MiB of key/value bytes, 1,000 SCAN calls, 16 keys per value batch and a five-second deadline. Literal matching escapes Redis glob grammar and post-filters keys. Values use atomic bounded GETRANGE batches; limit failures return no partial result or matched-key list. Empty string values, missing/non-string keys and ordinary small results retain their prior behavior.

Race-enabled acceptance passes the baseline regressions and normal/empty/type controls, small explicit key/byte/work budgets, bounded batch sizes, invalid settings and cancellation between scan batches. Only an owned Redis 8.6.1 fixture with synthetic data was used; it had no outbound network or host ports and was removed.

SCAN COUNT is a hint, so Redis's internal key traversal and individual SCAN replies are not absolute wire-memory bounds. The limits bound retained results, value responses and total cursor work. An embedding still needs rate/concurrency limits and appropriate Redis namespace ACLs. No production embedding, remote exploitability or deployed ACL policy is established by this repository.

## Atomic bounded lists: issue #12

Baseline accepts a six-element batch at a maximum of four, crosses a cap to length five and exposes lengths five/six under concurrent producers. RPush now rejects batches larger than the maximum, validates explicit trim sizes and atomically keeps recent existing items plus the complete accepted batch within the maximum. The old trimSize-plus-new-item behavior is retained when it fits. WithForceCheck remains source compatible; all operations enforce the bound. Zero payloads can compact an oversized list. Wrong-type and invalid input errors are returned before append.

## Acquisition-scoped leases: issues #10, #11 and #13

The forced baseline counter/insertion gap produces two successful holders with limit one. Shared-object locks reuse one token, collapse two permits into one member and allow stale cleanup to cancel/release a newer holder. Missing semaphore membership leaves the old context live. A valid existing ZADD XX also returns zero, so checking that integer alone would introduce a false ownership-loss bug.

Admission, prune/capacity decisions, renewal membership and token-specific release are atomic scripts. Every logical acquisition uses a fresh UUID and an immutable reference in its returned context. Parent-only Unlock calls fail closed while a lease is active. Explicit recursion uses Lock(existingLockCtx) and balanced references. Caller cancellation, expired scores, missing owners/membership and Redis errors revoke the context. Redis server time determines semaphore timestamps.

Migration: keep the context returned by Lock and pass it to Unlock. To recurse intentionally, pass that live context to Lock; do not reuse a parent context as implicit ownership. Update all clients sharing a Redis lock/semaphore name before relying on the repaired protocol; unsafe old clients cannot be fenced by a new library alone. No deployment was performed. Cooperative lease contexts do not replace downstream datastore fencing.

Full race acceptance passes ordinary tests and joined simultaneous-holder controls at limits 1, 2 and 4, balanced recursion, missing/mismatched/expired ownership, stale cleanup, cancellation and Redis-error controls. The prior semaphore race test spawned unjoined goroutines without measuring holders; its replacement joins bounded work and checks active critical sections.

Verify/vet/build pass. Source scan reports 0 called and 0 imported vulnerabilities, with unused-module GO-2026-5024. External review/check reconciliation is pending.

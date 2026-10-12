# Security finding remediation

## Prefix lookup: issues #9 and #14

Baseline revision e6207a7bb48d86265b8e8ee81444ff07bf812444 reproduces literal-prefix violations for *, ?, bracket expressions and backslash. A standalone ] already has literal Redis semantics. A synthetic 1,001-key lookup completes 95 SCAN calls, reaches cursor zero and sends one 1,001-key MGET. A value over 1 MiB is fully returned. An injected read error includes matched key names.

The existing GetItemWithPrefix signature remains. It now delegates to GetItemWithPrefixLimited with conservative defaults: 1,000 distinct inspected matching keys, 1 MiB of key/value bytes, 1,000 SCAN calls, 16 keys per value batch and a five-second deadline. Literal matching escapes Redis glob grammar and post-filters keys. Values use atomic bounded GETRANGE batches; limit failures return no partial result or matched-key list. Empty string values, missing/non-string keys and ordinary small results retain their prior behavior.

Race-enabled acceptance passes the baseline regressions and normal/empty/type controls, small explicit key/byte/work budgets, bounded batch sizes, invalid settings and cancellation between scan batches. Only an owned Redis 8.6.1 fixture with synthetic data was used; it had no outbound network or host ports and was removed.

SCAN COUNT is a hint, so Redis's internal key traversal and individual SCAN replies are not absolute wire-memory bounds. The limits bound retained results, value responses and total cursor work. An embedding still needs rate/concurrency limits and appropriate Redis namespace ACLs. No production embedding, remote exploitability or deployed ACL policy is established by this repository.

Remaining Redis findings #10–#13 are under separate incremental reproduction in this Draft. Full project acceptance and external review/check reconciliation are pending.

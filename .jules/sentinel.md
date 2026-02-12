## 2026-02-02 - Distributed Sync Stability and Panic Prevention
**Vulnerability:** Nil pointer dereference in `Unlock` methods and lack of context awareness in spin-locks.
**Learning:** Distributed lock implementations often involve background goroutines for heartbeats, managed via `context.CancelFunc`. If `Unlock` is called on an uninitialized or failed lock, it can lead to panics if the cancel function is not checked for nil.
**Prevention:** Always check for nil before calling `context.CancelFunc` stored in a struct. Use context-aware sleep functions (like `gutils.SleepWithContext`) in spin-loops to ensure resources are released immediately upon context cancellation.

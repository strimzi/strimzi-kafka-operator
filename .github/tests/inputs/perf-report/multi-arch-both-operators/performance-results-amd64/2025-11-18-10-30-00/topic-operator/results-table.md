**Use Case:** scalabilityUseCase

**Configuration:**
- IN: MAX QUEUE SIZE: 2147483647
- IN: MAX BATCH SIZE (ms): 100
- IN: MAX BATCH LINGER (ms): 100
- IN: PROCESS TYPE: TOPIC-CONCURRENT

**Results:**

| # | IN: NUMBER OF TOPICS | IN: NUMBER OF EVENTS | OUT: Reconciliation interval (ms) |
|---|---|---|---|
| 1 | 2 | 8 | 10229 |
| 2 | 32 | 98 | 11505 |
| 3 | 125 | 375 | 42367 |
| 4 | 250 | 750 | 74596 |
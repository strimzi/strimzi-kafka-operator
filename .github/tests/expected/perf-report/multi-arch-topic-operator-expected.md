## Performance Test Results

**Test Run:** `2025-11-18-10-30-00`

## Topic Operator

**Use Case:** scalabilityUseCase

**Configuration:**
- MAX QUEUE SIZE: 2147483647
- MAX BATCH SIZE (ms): 100
- MAX BATCH LINGER (ms): 100
- PROCESS TYPE: TOPIC-CONCURRENT

**Results:**

| # | NUMBER OF TOPICS | NUMBER OF EVENTS | Reconciliation interval (ms) [AMD64] | Reconciliation interval (ms) [ARM64] |
|---|---|---|---|---|
| 1 | 2 | 8 | 10229 | 10229 |
| 2 | 32 | 98 | 11505 | 11505 |
| 3 | 125 | 375 | 42367 | 42367 |
| 4 | 250 | 750 | 74596 | 74596 |

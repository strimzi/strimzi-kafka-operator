**Use Case:** scalabilityUseCase

**Configuration:**
- IN: WORK_QUEUE_SIZE: 1024
- IN: BATCH_MAXIMUM_BLOCK_SIZE: 100
- IN: BATCH_MAXIMUM_BLOCK_TIME_MS: 100

**Results:**

| # | IN: NUMBER OF KAFKA USERS | OUT: Reconciliation interval (ms) |
|---|---|---|
| 1 | 10 | 10472 |
| 2 | 100 | 33036 |
| 3 | 200 | 54940 |
| 4 | 500 | 133782 |

**Use Case:** latencyUseCase

**Configuration:**
- IN: WORK_QUEUE_SIZE: 2048
- IN: BATCH_MAXIMUM_BLOCK_SIZE: 100
- IN: BATCH_MAXIMUM_BLOCK_TIME_MS: 100

**Results:**

| # | IN: NUMBER OF KAFKA USERS | OUT: Min Latency (ms) | OUT: Max Latency (ms) | OUT: Average Latency (ms) | OUT: P50 Latency (ms) | OUT: P95 Latency (ms) | OUT: P99 Latency (ms) |
|---|---|---|---|---|---|---|---|
| 1 | 110 | 12 | 69 | 27.78 | 26 | 39 | 54 |
| 2 | 200 | 11 | 75 | 29.93 | 28 | 48 | 75 |
| 3 | 300 | 10 | 61 | 26.0 | 26 | 41 | 50 |
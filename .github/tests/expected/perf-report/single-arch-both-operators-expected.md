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

| # | NUMBER OF TOPICS | NUMBER OF EVENTS | Reconciliation interval (ms) |
|---|---|---|---|
| 1 | 2 | 8 | 10229 |
| 2 | 32 | 98 | 11505 |
| 3 | 125 | 375 | 42367 |
| 4 | 250 | 750 | 74596 |

## User Operator

**Use Case:** scalabilityUseCase

**Configuration:**
- WORK_QUEUE_SIZE: 1024
- BATCH_MAXIMUM_BLOCK_SIZE: 100
- BATCH_MAXIMUM_BLOCK_TIME_MS: 100

**Results:**

| # | NUMBER OF KAFKA USERS | Reconciliation interval (ms) |
|---|---|---|
| 1 | 10 | 10472 |
| 2 | 100 | 33036 |
| 3 | 200 | 54940 |
| 4 | 500 | 133782 |

**Use Case:** latencyUseCase

**Configuration:**
- WORK_QUEUE_SIZE: 2048
- BATCH_MAXIMUM_BLOCK_SIZE: 100
- BATCH_MAXIMUM_BLOCK_TIME_MS: 100

**Results:**

| # | NUMBER OF KAFKA USERS | Min Latency (ms) | Max Latency (ms) | Average Latency (ms) | P50 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) |
|---|---|---|---|---|---|---|---|
| 1 | 110 | 12 | 69 | 27.78 | 26 | 39 | 54 |
| 2 | 200 | 11 | 75 | 29.93 | 28 | 48 | 75 |
| 3 | 300 | 10 | 61 | 26.0 | 26 | 41 | 50 |

## Performance Test Results

**Test Run:** `2025-11-18-10-30-00`

## Topic Operator

**Use Case:** scalabilityUseCase

**Configuration:**
- IN: MAX QUEUE SIZE: 2147483647
- IN: MAX BATCH SIZE (ms): 100
- IN: MAX BATCH LINGER (ms): 100
- IN: PROCESS TYPE: TOPIC-CONCURRENT

**Results:**

| # | IN: NUMBER OF TOPICS | IN: NUMBER OF EVENTS | OUT: Reconciliation interval (ms) [AMD64] | OUT: Reconciliation interval (ms) [ARM64] |
|---|---|---|---|---|
| 1 | 2 | 8 | 10229 | 10229 |
| 2 | 32 | 98 | 11505 | 11505 |
| 3 | 125 | 375 | 42367 | 42367 |
| 4 | 250 | 750 | 74596 | 74596 |

## User Operator

**Use Case:** scalabilityUseCase

**Configuration:**
- IN: WORK_QUEUE_SIZE: 1024
- IN: BATCH_MAXIMUM_BLOCK_SIZE: 100
- IN: BATCH_MAXIMUM_BLOCK_TIME_MS: 100

**Results:**

| # | IN: NUMBER OF KAFKA USERS | OUT: Reconciliation interval (ms) [AMD64] | OUT: Reconciliation interval (ms) [ARM64] |
|---|---|---|---|
| 1 | 10 | 10472 | 10472 |
| 2 | 100 | 33036 | 33036 |
| 3 | 200 | 54940 | 54940 |
| 4 | 500 | 133782 | 133782 |

**Use Case:** latencyUseCase

**Configuration:**
- IN: WORK_QUEUE_SIZE: 2048
- IN: BATCH_MAXIMUM_BLOCK_SIZE: 100
- IN: BATCH_MAXIMUM_BLOCK_TIME_MS: 100

**Results:**

| # | IN: NUMBER OF KAFKA USERS | OUT: Min Latency (ms) [AMD64] | OUT: Min Latency (ms) [ARM64] | OUT: Max Latency (ms) [AMD64] | OUT: Max Latency (ms) [ARM64] | OUT: Average Latency (ms) [AMD64] | OUT: Average Latency (ms) [ARM64] | OUT: P50 Latency (ms) [AMD64] | OUT: P50 Latency (ms) [ARM64] | OUT: P95 Latency (ms) [AMD64] | OUT: P95 Latency (ms) [ARM64] | OUT: P99 Latency (ms) [AMD64] | OUT: P99 Latency (ms) [ARM64] |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 110 | 12 | 12 | 69 | 69 | 27.78 | 27.78 | 26 | 26 | 39 | 39 | 54 | 54 |
| 2 | 200 | 11 | 11 | 75 | 75 | 29.93 | 29.93 | 28 | 28 | 48 | 48 | 75 | 75 |
| 3 | 300 | 10 | 10 | 61 | 61 | 26.0 | 26.0 | 26 | 26 | 41 | 41 | 50 | 50 |
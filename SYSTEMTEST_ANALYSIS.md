# Strimzi Systemtest — Comprehensive Analysis

> Generated from deep analysis of the `systemtest/` module, Strimzi documentation, and kubetest4j/JUnit 5 capabilities.

---

## Table of Contents

1. [Module Overview](#1-module-overview)
2. [Current Test Infrastructure & Patterns](#2-current-test-infrastructure--patterns)
3. [Documentation vs. Test Coverage Gaps](#3-documentation-vs-test-coverage-gaps)
4. [Parallel Test Execution — Current State & Pain Points](#4-parallel-test-execution--current-state--pain-points)
5. [Namespace Handling — Current State & Pain Points](#5-namespace-handling--current-state--pain-points)
6. [kubetest4j & JUnit 5 — Current Usage & Missed Opportunities](#6-kubetest4j--junit-5--current-usage--missed-opportunities)
7. [Recommendations Summary](#7-recommendations-summary)

---

## 1. Module Overview

### 1.1 Test Files

The `systemtest/` module contains **60 test classes** across 16 major categories:

| Category | Files | Key Classes |
|----------|-------|-------------|
| `kafka/` | 8 | KafkaST, KafkaNodePoolST, KafkaVersionsST, QuotasST, TieredStorageST, ConfigProviderST, DynamicConfST, ListenersST |
| `operators/` | 10 | ClusterOperatorRbacST, FeatureGatesST, LeaderElectionST, MultipleClusterOperatorsST, PodSetST, ReconciliationST, RecoveryST, TopicST, UserST |
| `security/` | 8 | SecurityST, ClusterSecurityST, NetworkPoliciesST, PodSecurityProfilesST, CustomCaST, CustomCaChainST, OauthAuthorizationST, OauthPlainST |
| `rollingupdate/` | 4 | RollingUpdateST, KafkaRollerST, AlternativeReconcileTriggersST, InPlacePodResizingST |
| `bridge/` | 5 | HttpBridgeST, HttpBridgeCorsST, HttpBridgeTlsST, HttpBridgeScramShaST, HttpBridgeServerTlsST |
| `upgrade/` | 5 | KRaftStrimziUpgradeST, KRaftStrimziDowngradeST, KRaftKafkaUpgradeDowngradeST, KRaftOlmUpgradeST |
| `specific/` | 5 | DrainCleanerST, RackAwarenessST, HelmChartST, AccessOperatorST, RbacST |
| `cruisecontrol/` | 3 | CruiseControlST, CruiseControlApiST, CruiseControlConfigurationST |
| `metrics/` | 3 | MetricsST, JmxST, StrimziMetricsReporterST |
| `olm/` | 3 | OlmAllNamespaceST, OlmSingleNamespaceST |
| `watcher/` | 3 | AllNamespaceST, MultipleNamespaceST |
| `connect/` | 2 | ConnectST, ConnectBuilderST |
| `log/` | 2 | LogSettingST, LoggingChangeST |
| `mirrormaker/` | 1 | MirrorMaker2ST |
| `tracing/` | 1 | OpenTelemetryST |
| `performance/` | 4 | TopicOperatorPerformance, UserOperatorPerformance (scalability variants) |

### 1.2 Key Dependencies

| Dependency | Version | Notes |
|------------|---------|-------|
| `io.skodjob.kubetest4j:kubetest4j` | 1.3.0 | Core resource management extension |
| `org.junit.jupiter:junit-jupiter-api` | 6.0.0 | JUnit 5/6 (note: also using platform 6.0.0) |
| `io.fabric8:kubernetes-client` | (from parent pom) | Kubernetes API access |
| `io.skodjob.kubetest4j:metrics-collector` | 1.3.0 | Metrics collection helpers |
| `io.skodjob.kubetest4j:log-collector` | 1.3.0 | Log collection on failure |

### 1.3 Maven Profiles

`regression`, `smoke`, `sanity`, `acceptance`, `upgrade`, `kraft_upgrade`, `operators`, `brokers-and-security`, `operands`, `scalability`, `performance`, `user-capacity`, `topic-capacity`

---

## 2. Current Test Infrastructure & Patterns

### 2.1 AbstractST Lifecycle Chain

All 60 test classes extend `AbstractST` (directly or transitively). Its lifecycle is:

```
@BeforeAll setUpTestSuite()
  ├─ ThreadContext (logging) setup
  ├─ KubeResourceManager.setTestContext()
  ├─ beforeAllMayOverride()   → creates TEST_SUITE_NAMESPACE if needed
  └─ beforeAllMustExecute()   → verifies cluster is reachable

@BeforeEach setUpTestCase()
  ├─ ThreadContext setup
  ├─ KubeResourceManager.setTestContext()
  ├─ beforeEachMustExecute()  → acquires semaphore slot (for @Parallel* tests)
  └─ beforeEachMayOverride()  → synchronized → creates "namespace-N" (for @ParallelNamespaceTest)

--- TEST METHOD ---

@AfterEach tearDownTestCase()
  ├─ afterEachMayOverride()   → KubeResourceManager.deleteResources(true)
  └─ afterEachMustExecute()   → releases semaphore slot

@AfterAll tearDownTestSuite()
  ├─ afterAllMayOverride()    → synchronized → deleteResources + deleteTestSuiteNamespace
  └─ afterAllMustExecute()    → verifies cluster still healthy
```

### 2.2 Three Annotation Modes

| Annotation | Namespace | Execution | Lock Type | Use Case |
|-----------|-----------|-----------|-----------|----------|
| `@ParallelNamespaceTest` | Unique `namespace-N` per test | Concurrent | `READ` (shared) | Full cluster deploy (Kafka, Connect, etc.) |
| `@ParallelTest` | Shared `test-suite-namespace` | Concurrent | `READ` (shared) | Lightweight assertions, config checks |
| `@IsolatedTest` | Shared `test-suite-namespace` | Sequential | `READ_WRITE` (exclusive) | Cluster-wide state changes |

### 2.3 TestStorage Pattern

Every test method starts with:
```java
final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
```

`TestStorage` generates ~50 named test resources (cluster name, topic, producer/consumer names, label selectors, etc.) and stores them in the `ExtensionContext.Store`. This boilerplate appears in **312 call sites** across the codebase.

---

## 3. Documentation vs. Test Coverage Gaps

> **Scope note:** This section covers only gaps that require a live Kubernetes cluster and real Kafka/operator behaviour to validate — i.e. items that cannot be adequately covered by unit or integration tests. Operator-side validation logic (invalid config rejection, partition reduction, topic finalizer cleanup, etc.) is better tested at the unit/IT level and is excluded here.

### 3.1 Feature Coverage Matrix

| Documented Feature | Tests Exist | Coverage Quality | Gap Severity |
|-------------------|-------------|-----------------|--------------|
| **KRaft Cluster Deployment** | ✅ | Good (basics) | LOW |
| **KRaft Metadata Version Management** | ❌ | No dedicated version-transition tests | HIGH |
| **KRaft Controller Quorum Recovery** | ❌ | No failure injection at e2e level | HIGH |
| **Node Pool Role Assignment** | ✅ | Basic transitions tested | LOW |
| **Node Pool ID Gap Filling** | ✅ | Tested | LOW |
| **Node Pool Leader Election During Removal** | ❌ | Not tested | HIGH |
| **TLS Certificate Auto-Renewal** | ✅ | Well covered (expiry, annotation, maintenance window, mid-rolling) | LOW |
| **TLS Certificate Revocation (CRL/OCSP)** | ❌ | Zero tests — Kafka does support CRL via `ssl.crl.location` | HIGH |
| **mTLS + SCRAM Simultaneously on same cluster** | ❌ | Not tested | MEDIUM |
| **Custom CA Chain (multi-level)** | ✅ | Well covered | LOW |
| **FIPS Mode Compliance** | ℹ️ | Tests run on FIPS clusters; dedicated FIPS-specific assertions absent | LOW |
| **OAuth Token Refresh/Expiry** | ❌ | OauthPlainST / OauthAuthorizationST cover auth flows but not token lifecycle | MEDIUM |
| **OAuth JWKS Key Rotation** | ❌ | Not tested | MEDIUM |
| **NetworkPolicy Cross-Namespace Clients** | ❌ | Only same/different-namespace operator tests; client-level cross-NS not tested | MEDIUM |
| **Dynamic Config Invalid Value Rejection** | ℹ️ | Operator-side validation; better as unit test | — |
| **ConfigProvider Secret Rotation Mid-Run** | ❌ | Only happy path; secret update propagation not tested at e2e level | MEDIUM |
| **Tiered Storage Remote Backend Unavailability** | ❌ | Only happy path tested | HIGH |
| **Kafka Connect Connector Offset Management** | ❌ | No list/alter/reset tests (Kafka 3.5+ feature) | HIGH |
| **Kafka Connect Connector Task Lifecycle** | ⚠️ | Auto-restart only; pause/stop/resume task per-task not tested | MEDIUM |
| **Connect Builder Image Rebuild on Plugin Upgrade** | ❌ | Not tested | MEDIUM |
| **MirrorMaker2 Offset Sync Accuracy** | ❌ | Not validated | MEDIUM |
| **MirrorMaker2 ACL Synchronization** | ❌ | Not tested | MEDIUM |
| **Cruise Control: Rebalance with Active Producers** | ❌ | Not tested | HIGH |
| **Cruise Control: Goal Feasibility Failures** | ❌ | Not tested | MEDIUM |
| **Rolling Update: Continuous clients** | ✅ | Present in several tests (`testRecoveryDuringKRaftRollingUpdate`, `testManualTriggeringRollingUpdate`, `testAddingAndRemovingJbodVolumes`, upgrade tests) | LOW |
| **Rolling Update KRaft Controller Order** | ❌ | Rolling is validated; explicit controller-before-broker ordering not asserted | MEDIUM |
| **Pod Disruption Budget Enforcement** | ℹ️ | PDB is a Kubernetes-native feature; Strimzi sets it but enforcement is cluster-level | LOW |
| **Drain Cleaner** | ✅ | `DrainCleanerST.testDrainCleanerWithComponents()` covers webhook + continuous clients | LOW |
| **Feature Gates** | ⚠️ | Only 2 of ~10 gates have dedicated tests | MEDIUM |
| **Multi-CO Conflict Detection** | ❌ | Two operators watching same NS not tested | MEDIUM |
| **Leader Election During In-Flight Operations** | ❌ | `LeaderElectionST` tests failover, not mid-reconciliation safety | MEDIUM |
| **Helm Chart CRD Upgrade** | ❌ | Only single-version deployment tested | MEDIUM |
| **Tracing Context Propagation across MM2 + Bridge** | ❌ | Not tested | LOW |
| **Rack Awareness Multi-Zone Distribution** | ❌ | Configuration tested; actual replica distribution across zones not asserted | MEDIUM |

### 3.2 Priority-Ranked Missing Test Cases

Items below require a live cluster to validate and have no adequate unit/IT coverage.

#### Tier 1 — High value, currently untested at e2e level

1. **`testConnectorOffsetManagement`** — Connect connector offset list, alter, and reset (Kafka 3.5+ Admin API operations; requires live Connect cluster)
2. **`testTieredStorageRemoteBackendUnavailability`** — Remote backend goes down mid-sync; verify recovery and no data loss
3. **`testKRaftMetadataVersionTransition`** — Explicit metadata version upgrade with validation that brokers advertise correct version after transition
4. **`testCruiseControlRebalanceWithActiveProducers`** — Rebalance while producers write; verify partition movement completes and no messages lost
5. **`testMirrorMaker2OffsetSyncAccuracy`** — Validate offset parity between clusters after replication cycle
6. **`testMirrorMaker2ACLSync`** — ACL propagation from source to target cluster (requires live ACL inspection)
7. **`testCertificateRevocationCRL`** — Configure Kafka `ssl.crl.location`, revoke a client cert, verify broker rejects the TLS handshake

#### Tier 2 — Medium value, worth adding

8. **`testOAuthTokenExpiry`** — Let Keycloak token expire during active session; verify client transparently reconnects with refreshed token
9. **`testOAuthJWKSKeyRotation`** — Rotate Keycloak JWKS signing key; verify broker re-fetches and clients re-validate
10. **`testNodePoolLeaderElectionDuringRemoval`** — Remove broker node currently holding partition leadership; verify leadership handoff and no data loss
11. **`testConfigProviderSecretRotation`** — Update a referenced Secret while connector is running; verify new value is picked up
12. **`testMultiCOResourceConflict`** — Two Cluster Operators watch overlapping namespaces; verify no split-brain or duplicate reconciliation
13. **`testLeaderElectionDuringInFlightOperation`** — Kill leader CO pod while it is mid-reconciliation; verify the new leader completes the operation
14. **`testRollingUpdateKRaftControllerOrder`** — Explicitly assert that controllers complete their roll before broker pods begin rolling
15. **`testKRaftControllerQuorumRecovery`** — Kill majority of controller pods, verify quorum re-establishes and cluster becomes writable

---

## 4. Parallel Test Execution — Current State & Pain Points

### 4.1 Current Architecture

```
JUnit @ResourceLock(READ/READ_WRITE on "global")
     +
Custom SuiteThreadController Semaphore
     +
TestSuiteNamespaceManager (AtomicInteger counter)
```

The combination of JUnit's `@ResourceLock` AND a custom `Semaphore` creates a **dual-control system** where:
- JUnit decides which tests can run based on lock mode
- The Semaphore additionally limits concurrency to `junit.jupiter.execution.parallel.config.fixed.parallelism`

### 4.2 Pain Points

#### P1 — SuiteThreadController is Redundant and Introduces a Deadlock Surface

`SuiteThreadController` re-reads `junit.jupiter.execution.parallel.config.fixed.parallelism` from system properties and creates a `Semaphore` with that many permits. The intent is to cap how many `@Parallel*` tests run concurrently. However:

1. **JUnit already enforces this ceiling.** With `fixed.parallelism=N`, JUnit's `ForkJoinPool` has exactly N worker threads. A test can only execute if a thread is free. The Semaphore cannot allow more than N concurrent tests even without it — there are no threads available to run them.

2. **`@IsolatedTest` ↔ `@Parallel*` mutual exclusion is already provided by `@ResourceLock`.** `@IsolatedTest` carries `@ResourceLock(READ_WRITE, "global")` and `@Parallel*` tests carry `@ResourceLock(READ, "global")`. JUnit's lock manager guarantees an `@IsolatedTest` waits for all `READ` holders to finish, and blocks new `READ` acquisitions while it runs. The Semaphore adds nothing to this contract.

3. **The Semaphore introduces a deadlock path that JUnit's thread pool does not.** If a test's `@BeforeEach` acquires a semaphore slot but then hangs (slow Kubernetes API, namespace creation timeout), the slot is held and `@AfterEach` (`removeParallelTest` / `release()`) is never reached. Other tests queue on `acquire()` indefinitely. JUnit's native thread pool does not have this problem: a hung thread consumes a pool slot but does not prevent other threads from picking up other work.

**Proposed fix:** Delete `SuiteThreadController` entirely. Remove the `addParallelTest()` / `removeParallelTest()` calls from `AbstractST.beforeEachMustExecute()` and `afterEachMustExecute()`. Isolation and concurrency guarantees are fully preserved by `@ResourceLock` and `fixed.parallelism`.

#### P2 — MayOverride / MustExecute Naming Confusion
The split into `beforeEachMayOverride()` / `beforeEachMustExecute()` is not a JUnit primitive — it is a hand-rolled ordering guarantee. The names are misleading:
- "MayOverride" is called unconditionally from `setUpTestCase()`, making it mandatory from a lifecycle standpoint
- "MustExecute" also executes unconditionally but with a `try/finally`
- Developers maintaining subclasses must understand this invisible contract

#### P4 — `assertNoCoErrorsLogged()` Permanently Disabled
```java
// AbstractST.java:263 (commented out)
// assertNoCoErrorsLogged(); // brings flakiness and is unstable
```
This was removed because it was noisy. The underlying issue — false positive CO error detection — was never fixed. The quality check hole remains.

#### P5 — Static LOCK Serializes All Parallel Namespace Creation
```java
// AbstractST.java:186
synchronized (LOCK) {
    testSuiteNamespaceManager.createParallelNamespace();
}
```
Every `@ParallelNamespaceTest` acquires the **same global JVM-wide lock** to create its namespace. Namespace creation (which involves a Kubernetes API call with wait) therefore runs serially even when multiple test threads are active, creating unnecessary queuing.

#### P6 — KubeResourceManager Context Thread Safety Unknown
```java
// AbstractST.java:230, 244, 253, 273
KubeResourceManager.get().setTestContext(extensionContext);
```
This is called in every lifecycle method from potentially multiple threads. Whether `setTestContext` uses a ThreadLocal or a shared field determines if this is safe. If it is a shared field, the last writer wins — and tests could read a wrong `ExtensionContext`.

---

## 5. Namespace Handling — Current State & Pain Points

### 5.1 Namespace Lifecycle

```
TEST SUITE (BeforeAll)
  ├─ createTestSuiteNamespace()
  │  ├─ Only if suite contains @ParallelTest / @IsolatedTest tags
  │  ├─ Only if !Environment.isNamespaceRbacScope()
  │  └─ Creates "test-suite-namespace"
  └─ Name fixed: Environment.TEST_SUITE_NAMESPACE

EACH @ParallelNamespaceTest (BeforeEach)
  ├─ createParallelNamespace()
  │  ├─ Generates "namespace-" + counterOfNamespaces.getAndIncrement()
  │  ├─ Stores in ExtensionContext.Namespace.GLOBAL under NAMESPACE_KEY
  │  └─ NamespaceUtils.createNamespaceAndPrepare(name)
  │     ├─ Delete if exists (warning)
  │     ├─ Create namespace
  │     ├─ Apply default NetworkPolicies
  │     └─ Copy image pull secrets
  └─ Namespace name: "namespace-0", "namespace-1", ... "namespace-N"

EACH @ParallelNamespaceTest (AfterEach)
  └─ KubeResourceManager.deleteResources(true)
     ├─ Deletes all resources created in this test context
     └─ Namespace itself NOT explicitly deleted here (cleanup on re-use)

TEST SUITE (AfterAll)
  └─ deleteTestSuiteNamespace()
     └─ NamespaceUtils.deleteNamespace("test-suite-namespace")
```

### 5.2 Key Pain Points

#### N1 — Counter-Based Namespace Naming is Not Human-Readable

Namespaces are named `namespace-0`, `namespace-1`, ..., `namespace-N` via an `AtomicInteger`. Problems:
- **No reset between suites**: The `TestSuiteNamespaceManager` is a JVM singleton, so the counter continues from its last value across test class runs in the same JVM. Re-running a single test can produce `namespace-136` rather than `namespace-0`.
- **Not debuggable**: `namespace-47` cannot be associated with a test class or method without the run log.

**Proposed fix:** Use the unused `value()` attribute on `@ParallelNamespaceTest` to let each test declare its own stable, readable namespace name:

```java
// Test declares the namespace it wants — deterministic, readable, no generator
@ParallelNamespaceTest("kafka-roller")
void testKafkaPodCrashLooping() { ... }
```

`TestSuiteNamespaceManager.createParallelNamespace()` should use `value()` when set, and fall back to a short class-derived name (e.g. first 20 chars of class simple name, lowercased) only when `value()` is empty. This removes the counter entirely, the namespace name is stable across retries (same name → the cleanup-on-exists guard in `createNamespaceAndPrepare` handles any leftover from a previous crashed run), and logs are immediately readable.

#### N1a — Cluster Name (and Topic/User Names) are Similarly Opaque

The same readability issue exists for cluster names. [`TestStorage`](systemtest/src/main/java/io/strimzi/systemtest/storage/TestStorage.java:103) generates:

```java
this.clusterName = "cluster-" + hashStub(String.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)));
// e.g. "cluster-3f2a1b8c"
```

The 8-character SHA-1 stub of a random integer is opaque. In a parallel run with 4 test threads, the cluster operator logs and pod names contain `cluster-3f2a1b8c`, `cluster-7d9e0a2f`, etc. — impossible to map back to the test method without checking the run log.

Topic and user names have the same pattern (`my-topic-1234567-8901234`, `my-user-1234567-8901234`).

**Proposed fix:** Generate names from the test method name rather than a random number:

```java
// Short, stable, readable:  "cluster-kafkaroller-abc12"
// Method name (camelCase → kebab, truncated) + small counter suffix for local uniqueness
this.clusterName = "cluster-" + abbreviate(testName) + "-" + shortCounter();
```

The abbreviation needs to stay within Kubernetes 63-char label limits and DNS-safe characters. An alternative is to keep the hash but seed it from the test method name rather than `RANDOM`, making it deterministic across runs:

```java
this.clusterName = "cluster-" + hashStub(testName);  // same name every run for same method
```

#### N2 — Silent Behavior Change Under RBAC Mode
When `Environment.isNamespaceRbacScope()` is `true`, `@ParallelNamespaceTest` tests silently run in the shared `test-suite-namespace` instead of isolated namespaces, without any warning or error. Tests written expecting isolation will produce false positives or silent failures.

**Fix**: Log a prominent warning or throw an `AssumptionViolatedException` when namespace isolation is expected but not possible.

#### N3 — Global Lock Serializes Namespace Creation
The `synchronized (LOCK)` in `AbstractST.beforeEachMayOverride()` is a JVM-global lock shared by all test threads. Kubernetes namespace creation is an async API call followed by a wait-for-ready. This entire operation runs under the lock, serializing all parallel test setup even though the individual namespace creations are independent.

**Fix**: Remove the synchronized block. Each test's namespace name is already unique (via `AtomicInteger`); concurrent creation of different namespaces is safe.

#### N4 — Namespace Deletion Can Hang Indefinitely
`NamespaceUtils.deleteNamespace()` calls `KubeResourceManager.deleteResourceWithWait()` with no timeout override. If a namespace has stuck finalizers (a known Kubernetes issue), the wait loop never exits and the entire test suite hangs.

**Fix**: Add a timeout to namespace deletion, with a fallback that force-patches the namespace to remove finalizers.

#### N5 — TestStorage Namespace Resolution is Hidden
```java
// TestStorage.java:102
this.namespaceName = StUtils.isParallelNamespaceTest(extensionContext) ?
    StUtils.getNamespaceBasedOnRbac(namespaceName, extensionContext) : namespaceName;
```
A test developer passing a namespace name to `TestStorage` may not realize it can be silently overridden by the RBAC check. This hidden conditional is the source of subtle bugs where a test appears to run in one namespace but actually runs in another.

#### N6 — Repetitive `new TestStorage(...)` in Every Test Method
Every single test method (312 call sites) must manually instantiate `TestStorage` and then use `testStorage.getNamespaceName()`, `testStorage.getClusterName()`, etc. This is pure boilerplate with no value.

---

## 6. kubetest4j & JUnit 5 — Current Usage & Missed Opportunities

### 6.1 What Is Already Well Used

| Feature | Usage |
|---------|-------|
| `@ResourceManager` + `KubeResourceManager` | Deeply integrated; all resources created/deleted via it |
| `LogCollector` / `MetricsCollector` | Used for per-test artifact collection |
| `@ExtendWith(TestExecutionWatcher.class)` | Exception handling and log collection on failure |
| `@ParameterizedTest` + `@MethodSource` | Used in 6 test suites for data-driven testing |
| `@TestMethodOrder` + `@Order` | OLM and OAuth ordered suites |
| `@Tag` | Pervasive filtering (REGRESSION, SANITY, ACCEPTANCE, etc.) |
| Custom `ExecutionCondition` annotations | 9 conditions: `@OpenShiftOnly`, `@FIPSNotSupported`, `@MultiNodeClusterOnly`, etc. |

### 6.2 Features NOT Used — With Adoption Recommendations

#### A — `ParameterResolver` for TestStorage Injection (HIGH VALUE)

**Current:**
```java
@ParallelNamespaceTest
void testJvmAndResources() {
    final TestStorage ts = new TestStorage(KubeResourceManager.get().getTestContext());
    // ... 200 lines using ts
}
```
**Proposed:** Implement a `TestStorageParameterResolver` JUnit 5 extension:
```java
@ParallelNamespaceTest
void testJvmAndResources(TestStorage ts) {  // ← injected automatically
    // ... 200 lines using ts
}
```
This eliminates 312 boilerplate call sites, removes the global `KubeResourceManager.getTestContext()` dependency from test code, and gives IDE autocompletion.

#### B — `BeforeEachCallback` / `AfterEachCallback` Extensions for Namespace Lifecycle (HIGH VALUE)

**Current:** Namespace creation is wired into `AbstractST.beforeEachMayOverride()` as a synchronized static method call — a hand-rolled extension point.

**Proposed:** Create a `NamespaceExtension implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback`. This is exactly what JUnit 5 `@ExtendWith` extensions are designed for:

```java
public class NamespaceExtension implements BeforeAllCallback, AfterAllCallback,
                                            BeforeEachCallback, AfterEachCallback {
    @Override
    public void beforeAll(ExtensionContext ctx) {
        if (requiresSharedNamespace(ctx)) {
            createTestSuiteNamespace(ctx);
        }
    }

    @Override
    public void beforeEach(ExtensionContext ctx) {
        if (isParallelNamespaceTest(ctx)) {
            String ns = generateUniqueNamespace(ctx);  // UUID-based
            createNamespaceAndPrepare(ns);
            ctx.getStore(SCOPE).put(NAMESPACE_KEY, ns);
        }
    }

    @Override
    public void afterEach(ExtensionContext ctx) {
        // namespace cleanup is handled by KubeResourceManager stack (no-op needed)
    }

    @Override
    public void afterAll(ExtensionContext ctx) {
        deleteTestSuiteNamespace(ctx);
    }
}
```

Benefits:
- Removes the `synchronized (LOCK)` block (each test gets its own UUID namespace; no conflict)
- Removes `TestSuiteNamespaceManager` singleton
- Makes `AbstractST`'s `beforeEachMayOverride` / `afterEachMayOverride` chain unnecessary

#### C — `@RegisterExtension` for Scoped Log Collection (MEDIUM VALUE)

`TestExecutionWatcher` is registered as a class-level `@ExtendWith`, making it global. For cases where log collection scope should differ per test class, `@RegisterExtension` (instance-level) would allow per-class customization:

```java
@RegisterExtension
final TestLogCollector logCollector = TestLogCollector.builder()
    .namespace(testStorage.getNamespaceName())
    .components(KafkaComponents.KAFKA, KafkaComponents.CONNECT)
    .build();
```

#### D — `@Nested` Classes for Large ST Files (MEDIUM VALUE)

`MetricsST.java`, `SecurityST.java`, `ConnectST.java`, and `MirrorMaker2ST.java` are very large (600–1000+ lines). JUnit 5's `@Nested` would allow logical groupings that share `@BeforeEach` state while producing hierarchical test report output:

```java
class MetricsST extends AbstractST {
    @Nested
    class BrokerMetrics {
        @ParallelNamespaceTest
        void testKafkaMetrics() { ... }
    }

    @Nested
    class ExporterMetrics {
        @ParallelNamespaceTest
        void testKafkaExporterMetrics() { ... }
    }
}
```

#### E — `Store.CloseableResource` for Automatic Resource Cleanup (LOW-MEDIUM VALUE)

Resources registered as `CloseableResource` in the JUnit store are automatically cleaned up at scope end. This could replace the manual `deleteResources(true)` call in `afterEachMayOverride`:

```java
ctx.getStore(SCOPE).put("kafka-cluster", (CloseableResource) () -> {
    KubeResourceManager.get().deleteResources(true);
});
```

#### F — `@TempDir` for Certificate/Keystore Files (LOW VALUE)

Tests that create temp files for cert/keystore handling call `Files.createTempDirectory()` manually. JUnit 5's `@TempDir` injection would clean these up automatically and reduce file leak risk.

#### G — Eliminate Dual Singleton: `KubeClusterResource` vs `KubeResourceManager`

There are **21 call sites** to `KubeClusterResource.getInstance()` — a separate Kubernetes client singleton from `KubeResourceManager`. All of these could be replaced with `KubeResourceManager.get().kubeClient()`, eliminating one source-of-truth for cluster metadata.

#### H — Remove `SuiteThreadController` Semaphore (MEDIUM VALUE)

The `SuiteThreadController` Semaphore re-implements what `junit.jupiter.execution.parallel.config.fixed.parallelism` already does. With correct JUnit 5 configuration, the platform's own executor pool limits concurrency. The Semaphore adds:
- An extra blocking call per test
- A potential deadlock surface (no timeout on `acquire()`)
- Confusion about what actually controls parallelism

Removing it and relying solely on JUnit's parallel config would simplify the framework and eliminate the deadlock risk.

---

## 7. Recommendations Summary

### 7.1 Immediate Actions (Framework Improvements)

| # | Action | Impact | Effort |
|---|--------|--------|--------|
| F1 | Add `tryAcquire(timeout)` to `SuiteThreadController` or remove Semaphore | Prevents hangs | Low |
| F2 | Remove `synchronized (LOCK)` from `beforeEachMayOverride()` — namespace names are already unique | Removes serialization bottleneck | Low |
| F3 | Replace counter-based `namespace-N` names with `ns-{class}-{uuid8}` format | Human-readable, collision-free | Low |
| F4 | Add timeout + finalizer-patch fallback to `NamespaceUtils.deleteNamespace()` | Prevents infinite hang on stuck finalizer | Medium |
| F5 | Add a warning log when `@ParallelNamespaceTest` silently falls back to shared namespace in RBAC mode | Surfaces silent behavior change | Low |
| F6 | Verify (and document) whether `KubeResourceManager.setTestContext()` is thread-safe | Ensures correctness under parallelism | Low |

### 7.2 Short-Term JUnit 5 Adoption

| # | Action | Impact | Effort |
|---|--------|--------|--------|
| J1 | Implement `NamespaceExtension` (`BeforeAllCallback`, `BeforeEachCallback`, etc.) to replace `TestSuiteNamespaceManager` | Removes static singletons, removes global lock | High |
| J2 | Implement `TestStorageParameterResolver` to inject `TestStorage` as method parameters | Eliminates 312 boilerplate call sites | High |
| J3 | Migrate `KubeClusterResource` usages to `KubeResourceManager.get().kubeClient()` | Eliminates dual-singleton confusion | Medium |
| J4 | Split large ST files (`MetricsST`, `SecurityST`, `MirrorMaker2ST`) into `@Nested` class groups | Better test report structure, easier navigation | Medium |

### 7.3 Test Coverage — Critical Gaps to Close

Ordered by risk to production users:

| Priority | Test to Add | Feature | Why Critical |
|----------|------------|---------|--------------|
| P1 | `testZeroDowntimeRollingUpdate` | Rolling Updates | Message loss during updates is a production incident |
| P1 | `testPodDisruptionBudgetEnforcement` | PDB | PDB misconfiguration causes unexpected downtime |
| P1 | `testCertificateRotationMidClientRequest` | TLS | Cert rotation dropping active connections is silent data loss |
| P1 | `testTieredStorageRemoteBackendUnavailability` | Tiered Storage | Remote backend failure path is completely untested |
| P1 | `testKRaftMetadataVersionTransition` | KRaft | Metadata version is user-facing; incompatible versions break clusters |
| P2 | `testCertificateRevocationCRL` | Security | Revoked certs not being rejected is a security vulnerability |
| P2 | `testFIPSModeBasicCompliance` | FIPS | FIPS is a compliance requirement; zero test coverage is a gap |
| P2 | `testConnectorOffsetManagement` | Connect | New Kafka 3.5+ feature with no test |
| P2 | `testMirrorMaker2OffsetSyncAccuracy` | MM2 | Offset drift goes undetected without precise validation |
| P2 | `testCruiseControlRebalanceWithActiveProducers` | Cruise Control | Rebalance with active production is a common production scenario |
| P2 | `testOAuthTokenExpiry` | OAuth | Token lifecycle not tested; expiry causes auth failures in production |
| P2 | `testLeaderElectionDuringInFlightOperation` | Operator HA | Mid-reconciliation leader failover is a gap in HA testing |
| P2 | `testRollingUpdateKRaftControllerOrder` | KRaft | Controller rolling order affects cluster availability |
| P2 | `testNodePoolLeaderElectionDuringRemoval` | Node Pools | Leadership handoff during pool removal is untested |
| P3 | `testDynamicConfigInvalidValueRejection` | Config | Operator should surface Kafka rejection cleanly |
| P3 | `testHelmChartCRDUpgrade` | Helm | CRD upgrade is a common user operation with no test |
| P3 | `testNetworkPolicyCrossNamespaceClient` | NetworkPolicy | Real-world cross-namespace access is not validated |
| P3 | `testMultiCOResourceConflict` | Multi-CO | Two operators fighting over same resource is a known risk |
| P3 | `testConfigProviderSecretRotation` | ConfigProvider | Secret updates should propagate without restart |
| P3 | `testTopicPartitionReductionRejected` | Topic Operator | Partition reduction should produce a clear error status |

---

*Analysis based on: 60 test class files, Strimzi documentation AsciiDoc sources, kubetest4j 1.3.0 API, JUnit Jupiter 6.0.0 API.*

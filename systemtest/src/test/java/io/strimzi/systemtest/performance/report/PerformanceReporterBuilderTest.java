/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils.LatencyMetrics;
import io.strimzi.systemtest.storage.TestStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PerformanceReporterBuilderTest {
    private final UserOperatorPerformanceReporter userReporter = new UserOperatorPerformanceReporter();
    private final TopicOperatorPerformanceReporter topicReporter = new TopicOperatorPerformanceReporter();
    private final Map<Long, Map<String, List<Double>>> history = Map.of(123L, Map.of("metric", List.of(1.0, 2.0)));
    private static final TemporalAccessor FIXED_TIME = LocalDateTime.of(2026, 1, 2, 3, 4, 5);
    private static final String KAFKA_YAML = "spec:\n  kafka:\n    version: 4.0.0\n";

    @FunctionalInterface
    private interface ReportCall {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface FluentReportCall {
        void run(Path root) throws Exception;
    }

    @TempDir
    Path tempDir;

    private static class OfflineUserReporter extends UserOperatorPerformanceReporter {
        private final Path baseDir;
        private OfflineUserReporter(Path baseDir) {
            this.baseDir = baseDir;
        }
        @Override protected String serializeKafkaConfiguration(String namespace, String clusterName) {
            return KAFKA_YAML;
        }
        @Override
        protected TemporalAccessor performanceTimestamp() {
            return FIXED_TIME;
        }
        @Override
        protected String performanceBaseDirectory() {
            return baseDir.toString();
        }
    }

    private static class OfflineTopicReporter extends TopicOperatorPerformanceReporter {
        private final Path baseDir;
        private OfflineTopicReporter(Path baseDir) {
            this.baseDir = baseDir;
        }
        @Override protected String serializeKafkaConfiguration(String namespace, String clusterName) {
            return KAFKA_YAML;
        }
        @Override
        protected TemporalAccessor performanceTimestamp() {
            return FIXED_TIME;
        }
        @Override
        protected String performanceBaseDirectory() {
            return baseDir.toString();
        }
    }

    private static TestStorage storage() {
        Map<String, Object> values = new LinkedHashMap<>();
        ExtensionContext.Store store = (ExtensionContext.Store) Proxy.newProxyInstance(
            ExtensionContext.Store.class.getClassLoader(), new Class<?>[]{ExtensionContext.Store.class},
            (proxy, method, args) -> {
                if (method.getName().equals("put")) {
                    values.put(args[0].toString(), args[1]);
                    return null;
                }
                if (method.getName().equals("get")) {
                    return values.get(args[0].toString());
                }
                return null;
            });
        ExtensionContext context = (ExtensionContext) Proxy.newProxyInstance(
            ExtensionContext.class.getClassLoader(), new Class<?>[]{ExtensionContext.class},
            (proxy, method, args) -> {
                if (method.getName().equals("getStore")) {
                    return store;
                }
                if (method.getName().equals("getTestMethod")) {
                    return Optional.empty();
                }
                if (method.getName().equals("getElement")) {
                    return Optional.of(PerformanceReporterBuilderTest.class);
                }
                return null;
            });
        return new TestStorage(context, "offline-ns");
    }

    private static void assertTreesEqual(Path expected, Path actual) throws Exception {
        List<Path> expectedFiles;
        try (Stream<Path> paths = Files.walk(expected)) {
            expectedFiles = paths.filter(Files::isRegularFile).sorted().toList();
        }
        List<Path> actualFiles;
        try (Stream<Path> paths = Files.walk(actual)) {
            actualFiles = paths.filter(Files::isRegularFile).sorted().toList();
        }
        assertEquals(expectedFiles.stream().map(expected::relativize).toList(), actualFiles.stream().map(actual::relativize).toList());
        for (Path relative : expectedFiles.stream().map(expected::relativize).toList()) {
            assertArrayEquals(Files.readAllBytes(expected.resolve(relative)), Files.readAllBytes(actual.resolve(relative)), relative.toString());
            assertEquals("2026-01-02-03-04-05", relative.getName(0).toString());
            if (relative.getFileName().toString().equals(PerformanceConstants.PERFORMANCE_METRICS_FILE_NAME + ".txt")) {
                assertTrue(Files.readString(actual.resolve(relative)).endsWith(PerformanceConstants.KAFKA_IN_CONFIGURATION + "=\n" + KAFKA_YAML));
            }
        }
    }

    private static List<Path> files(Path root) throws Exception {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).sorted().toList();
        }
    }

    private static void assertValuesAndTypes(Map<String, Object> actual, List<Object> expectedValues, List<Class<?>> expectedTypes) {
        assertEquals(expectedValues, List.copyOf(actual.values()));
        assertEquals(expectedTypes, actual.values().stream().map(Object::getClass).toList());
    }

    @Test
    void allBuilderTerminalMethodsMatchLegacyReportFiles() throws Exception {
        TestStorage storage = storage();
        assertEquivalent("topic-capacity", (root) -> {
            OfflineTopicReporter reporter = new OfflineTopicReporter(root);
            Map<String, Object> legacy = new LinkedHashMap<>();
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE, "2147483647");
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, "100");
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, "100");
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_OUT_SUCCESSFUL_KAFKA_TOPICS_CREATED, 5);
            legacy.put(PerformanceConstants.METRICS_HISTORY, history);
            return () -> reporter.logPerformanceData(storage, legacy, "topic-operator/" + PerformanceConstants.GENERAL_CAPACITY_USE_CASE, FIXED_TIME, root.toString());
        }, (root) -> new OfflineTopicReporter(root).capacity().maxQueueSize("2147483647").maxBatchSize("100")
            .maxBatchLingerMs("100").successfulCreations(5).metricsHistory(history).log(storage));

        assertEquivalent("topic-scalability", (root) -> {
            OfflineTopicReporter reporter = new OfflineTopicReporter(root);
            Map<String, Object> legacy = new LinkedHashMap<>();
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE, Integer.MAX_VALUE);
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, 100);
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS, 2);
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_EVENTS, 8);
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, 100);
            legacy.put(PerformanceConstants.TOPIC_OPERATOR_IN_PROCESS_TYPE, "TOPIC-CONCURRENT");
            legacy.put(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL, 123L);
            return () -> reporter.logPerformanceData(storage, legacy, "topic-operator/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE, FIXED_TIME, root.toString());
        }, (root) -> new OfflineTopicReporter(root).scalability().maxQueueSize(Integer.MAX_VALUE).maxBatchSize(100)
            .numberOfTopics(2).numberOfEvents(8).maxBatchLingerMs(100).processType("TOPIC-CONCURRENT")
            .reconciliationIntervalMs(123L).log(storage));

        assertEquivalent("user-capacity", (root) -> {
            OfflineUserReporter reporter = new OfflineUserReporter(root);
            Map<String, Object> legacy = new LinkedHashMap<>();
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS, "300000");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, "10000");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_CONTROLLER_THREAD_POOL_SIZE, "4");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_CACHE_REFRESH_INTERVAL_MS, "1000");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_QUEUE_SIZE, "10");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, "20");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, "30");
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE, "4");
            legacy.put(PerformanceConstants.USER_OPERATOR_OUT_SUCCESSFUL_KAFKA_USERS_CREATED, 7);
            legacy.put(PerformanceConstants.METRICS_HISTORY, history);
            return () -> reporter.logPerformanceData(storage, legacy, "user-operator/" + PerformanceConstants.GENERAL_CAPACITY_USE_CASE, FIXED_TIME, root.toString());
        }, (root) -> new OfflineUserReporter(root).capacity().operationTimeoutMs("300000").workQueueSize("10000")
            .controllerThreadPoolSize("4").cacheRefreshIntervalMs("1000").batchQueueSize("10")
            .batchMaximumBlockSize("20").batchMaximumBlockTimeMs("30")
            .successfulCreations(7).metricsHistory(history).log(storage));

        assertEquivalent("user-scalability", (root) -> {
            OfflineUserReporter reporter = new OfflineUserReporter(root);
            Map<String, Object> legacy = new LinkedHashMap<>();
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, 1024);
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, 100);
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, 10);
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, 100);
            legacy.put(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL, 123L);
            return () -> reporter.logPerformanceData(storage, legacy, "user-operator/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE, FIXED_TIME, root.toString());
        }, (root) -> new OfflineUserReporter(root).scalability().workQueueSize(1024).batchMaximumBlockSize(100)
            .numberOfKafkaUsers(10).batchMaximumBlockTimeMs(100).reconciliationIntervalMs(123L).log(storage));

        assertEquivalent("user-latency", (root) -> {
            OfflineUserReporter reporter = new OfflineUserReporter(root);
            LatencyMetrics latency = new LatencyMetrics(1, 9, 4.5, 2, 8, 9);
            Map<String, Object> legacy = new LinkedHashMap<>();
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, 4096);
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, 100);
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, 1000);
            legacy.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, 100);
            legacy.put(PerformanceConstants.OPERATOR_OUT_MIN_LATENCY, latency.min());
            legacy.put(PerformanceConstants.OPERATOR_OUT_MAX_LATENCY, latency.max());
            legacy.put(PerformanceConstants.OPERATOR_OUT_AVERAGE_LATENCY, latency.average());
            legacy.put(PerformanceConstants.OPERATOR_OUT_P50_LATENCY, latency.p50());
            legacy.put(PerformanceConstants.OPERATOR_OUT_P95_LATENCY, latency.p95());
            legacy.put(PerformanceConstants.OPERATOR_OUT_P99_LATENCY, latency.p99());
            legacy.put(PerformanceConstants.METRICS_HISTORY, history);
            return () -> reporter.logPerformanceData(storage, legacy, "user-operator/" + PerformanceConstants.GENERAL_LATENCY_USE_CASE, FIXED_TIME, root.toString());
        }, (root) -> new OfflineUserReporter(root).latency().workQueueSize(4096).batchMaximumBlockSize(100)
            .numberOfKafkaUsers(1000).batchMaximumBlockTimeMs(100).latencyMetrics(new LatencyMetrics(1, 9, 4.5, 2, 8, 9))
            .metricsHistory(history).log(storage));
    }

    @Test
    void metricsHistoryAppendsAndAbsentHistoryCreatesNoHistoryFiles() throws Exception {
        TestStorage storage = storage();
        Path root = tempDir.resolve("history-output");
        OfflineTopicReporter reporter = new OfflineTopicReporter(root);
        reporter.capacity().maxQueueSize("1").maxBatchSize("2").maxBatchLingerMs("3")
            .metricsHistory(Map.of(123L, Map.of("metric", List.of(1.0, 2.0)))).log(storage);
        reporter.capacity().maxQueueSize("1").maxBatchSize("2").maxBatchLingerMs("3")
            .metricsHistory(Map.of(456L, Map.of("metric", List.of(3.0)))).log(storage);

        Path useCaseDir = root.resolve("2026-01-02-03-04-05/topic-operator/capacityUseCase/max-batch-size-2-max-linger-time-3-with-clients-false");
        Path historyFile = useCaseDir.resolve("metric.txt");
        String historyText = Files.readString(historyFile);
        assertEquals(2, historyText.lines().count());
        assertTrue(historyText.contains("Timestamp= " + historyTimestamp(123L) + ", Values= [1.0, 2.0]"));
        assertTrue(historyText.contains("Timestamp= " + historyTimestamp(456L) + ", Values= [3.0]"));

        Path withoutHistory = tempDir.resolve("history-absent");
        new OfflineTopicReporter(withoutHistory).scalability().maxQueueSize(1).maxBatchSize(2)
            .numberOfTopics(0).numberOfEvents(0).maxBatchLingerMs(3).processType("TOPIC-CONCURRENT")
            .reconciliationIntervalMs(0L).log(storage);
        assertEquals(List.of("test-performance-metrics.txt"), files(withoutHistory).stream()
            .map(path -> path.getFileName().toString()).toList());
    }

    @Test
    void reportIOExceptionPropagatesAndMetricsHistoryIOExceptionIsLogged() throws Exception {
        TestStorage storage = storage();
        Path invalidBase = tempDir.resolve("not-a-directory");
        Files.writeString(invalidBase, "occupied");
        OfflineTopicReporter invalidReporter = new OfflineTopicReporter(invalidBase);
        assertThrows(IOException.class, () -> invalidReporter.capacity().maxQueueSize("1").maxBatchSize("2")
            .maxBatchLingerMs("3").metricsHistory(Map.of()).log(storage));

        Path root = tempDir.resolve("history-write-failure");
        Path useCaseDir = root.resolve("2026-01-02-03-04-05/topic-operator/capacityUseCase/max-batch-size-2-max-linger-time-3-with-clients-false");
        Files.createDirectories(useCaseDir.resolve("metric.txt"));
        OfflineTopicReporter reporter = new OfflineTopicReporter(root);
        assertDoesNotThrow(() -> reporter.capacity().maxQueueSize("1").maxBatchSize("2").maxBatchLingerMs("3")
            .metricsHistory(Map.of(123L, Map.of("metric", List.of(1.0)))).log(storage));
        assertTrue(Files.isRegularFile(useCaseDir.resolve("test-performance-metrics.txt")));
    }

    private static String historyTimestamp(long epochMillis) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSS")
            .withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(epochMillis));
    }

    private void assertEquivalent(String name, Function<Path, ReportCall> legacy, FluentReportCall fluent) throws Exception {
        Path legacyRoot = tempDir.resolve(name + "-legacy");
        Path fluentRoot = tempDir.resolve(name + "-fluent");
        legacy.apply(legacyRoot).run();
        fluent.run(fluentRoot);
        assertTreesEqual(legacyRoot, fluentRoot);
        assertTrue(!files(fluentRoot).isEmpty());
    }


    @Test
    void userCapacityPreservesLegacyMapContract() {
        Map<String, Object> actual = userReporter.capacity()
            .operationTimeoutMs("300000").workQueueSize("10000").controllerThreadPoolSize("4")
            .cacheRefreshIntervalMs("1000").batchQueueSize("10").batchMaximumBlockSize("20")
            .batchMaximumBlockTimeMs("30")
            .successfulCreations(7).metricsHistory(history).attributes();

        assertEquals(List.of(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS,
            PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_CONTROLLER_THREAD_POOL_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_CACHE_REFRESH_INTERVAL_MS,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_QUEUE_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS,
            PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE,
            PerformanceConstants.USER_OPERATOR_OUT_SUCCESSFUL_KAFKA_USERS_CREATED,
            PerformanceConstants.METRICS_HISTORY), List.copyOf(actual.keySet()));
        assertEquals(String.class, actual.get(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS).getClass());
        assertEquals(Integer.class, actual.get(PerformanceConstants.USER_OPERATOR_OUT_SUCCESSFUL_KAFKA_USERS_CREATED).getClass());
        assertEquals("4", actual.get(PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE));
        assertSame(history, actual.get(PerformanceConstants.METRICS_HISTORY));
        assertNull(userReporter.capacity().attributes().get(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS));
        assertValuesAndTypes(actual, List.of("300000", "10000", "4", "1000", "10", "20", "30", "4", 7, history),
            List.of(String.class, String.class, String.class, String.class, String.class, String.class,
                String.class, String.class, Integer.class, history.getClass()));
    }

    @Test
    void userScalabilityPreservesLegacyMapContract() {
        Map<String, Object> actual = userReporter.scalability().workQueueSize(1024).batchMaximumBlockSize(100)
            .numberOfKafkaUsers(10).batchMaximumBlockTimeMs(100).reconciliationIntervalMs(123L).attributes();
        assertEquals(List.of(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS,
            PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL), List.copyOf(actual.keySet()));
        assertEquals(Integer.class, actual.get(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE).getClass());
        assertEquals(Long.class, actual.get(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL).getClass());
        assertValuesAndTypes(actual, List.of(1024, 100, 10, 100, 123L),
            List.of(Integer.class, Integer.class, Integer.class, Integer.class, Long.class));
        assertEquals(0, userReporter.scalability().attributes().get(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE));
    }

    @Test
    void userLatencyPreservesLegacyMapContractAndTypes() {
        LatencyMetrics latency = new LatencyMetrics(1, 9, 4.5, 2, 8, 9);
        Map<String, Object> actual = userReporter.latency().workQueueSize(4096).batchMaximumBlockSize(100)
            .numberOfKafkaUsers(1000).batchMaximumBlockTimeMs(100).latencyMetrics(latency)
            .metricsHistory(history).attributes();
        assertEquals(List.of(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE,
            PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS,
            PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS,
            PerformanceConstants.OPERATOR_OUT_MIN_LATENCY, PerformanceConstants.OPERATOR_OUT_MAX_LATENCY,
            PerformanceConstants.OPERATOR_OUT_AVERAGE_LATENCY, PerformanceConstants.OPERATOR_OUT_P50_LATENCY,
            PerformanceConstants.OPERATOR_OUT_P95_LATENCY, PerformanceConstants.OPERATOR_OUT_P99_LATENCY,
            PerformanceConstants.METRICS_HISTORY), List.copyOf(actual.keySet()));
        assertEquals(Long.class, actual.get(PerformanceConstants.OPERATOR_OUT_MIN_LATENCY).getClass());
        assertEquals(Double.class, actual.get(PerformanceConstants.OPERATOR_OUT_AVERAGE_LATENCY).getClass());
        assertSame(history, actual.get(PerformanceConstants.METRICS_HISTORY));
        assertValuesAndTypes(actual, List.of(4096, 100, 1000, 100, 1L, 9L, 4.5, 2L, 8L, 9L, history),
            List.of(Integer.class, Integer.class, Integer.class, Integer.class, Long.class, Long.class,
                Double.class, Long.class, Long.class, Long.class, history.getClass()));
        Map<String, Object> freshBuilder = userReporter.latency().workQueueSize(1).latencyMetrics(new LatencyMetrics(10, 20, 15.0, 12, 18, 19)).attributes();
        assertEquals(10L, freshBuilder.get(PerformanceConstants.OPERATOR_OUT_MIN_LATENCY));
        assertEquals(4096, actual.get(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE));
    }

    @Test
    void topicCapacityPreservesLegacyMapContractAndFreshBuilders() {
        TopicOperatorPerformanceReporter.TopicCapacityBuilder first = topicReporter.capacity().maxQueueSize("2147483647")
            .maxBatchSize("100").maxBatchLingerMs("100").successfulCreations(5).metricsHistory(history);
        Map<String, Object> actual = first.attributes();
        Map<String, Object> second = topicReporter.capacity().maxQueueSize("1").attributes();
        assertEquals(List.of(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE,
            PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE,
            PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS,
            PerformanceConstants.TOPIC_OPERATOR_OUT_SUCCESSFUL_KAFKA_TOPICS_CREATED,
            PerformanceConstants.METRICS_HISTORY), List.copyOf(actual.keySet()));
        assertEquals(String.class, actual.get(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE).getClass());
        assertEquals(Integer.class, actual.get(PerformanceConstants.TOPIC_OPERATOR_OUT_SUCCESSFUL_KAFKA_TOPICS_CREATED).getClass());
        assertSame(history, actual.get(PerformanceConstants.METRICS_HISTORY));
        assertNotSame(actual, second);
        assertEquals("1", second.get(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE));
        assertValuesAndTypes(actual, List.of("2147483647", "100", "100", 5, history),
            List.of(String.class, String.class, String.class, Integer.class, history.getClass()));
        assertNull(topicReporter.capacity().attributes().get(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE));
    }

    @Test
    void topicScalabilityPreservesLegacyMapContract() {
        Map<String, Object> actual = topicReporter.scalability().maxQueueSize(Integer.MAX_VALUE).maxBatchSize(100)
            .numberOfTopics(2).numberOfEvents(8).maxBatchLingerMs(100).processType("TOPIC-CONCURRENT")
            .reconciliationIntervalMs(123L).attributes();
        assertEquals(List.of(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE,
            PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE,
            PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS,
            PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_EVENTS,
            PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS,
            PerformanceConstants.TOPIC_OPERATOR_IN_PROCESS_TYPE,
            PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL), List.copyOf(actual.keySet()));
        assertEquals(Integer.class, actual.get(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE).getClass());
        assertEquals(Long.class, actual.get(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL).getClass());
        assertEquals("TOPIC-CONCURRENT", actual.get(PerformanceConstants.TOPIC_OPERATOR_IN_PROCESS_TYPE));
        assertValuesAndTypes(actual, List.of(Integer.MAX_VALUE, 100, 2, 8, 100, "TOPIC-CONCURRENT", 123L),
            List.of(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, String.class, Long.class));
        assertEquals(0, topicReporter.scalability().attributes().get(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE));
    }
}

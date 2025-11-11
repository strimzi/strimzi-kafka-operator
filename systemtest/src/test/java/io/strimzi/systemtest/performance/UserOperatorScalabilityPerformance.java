/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.performance.report.UserOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.UserOperatorMetricsParser;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils.LatencyMetrics;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@SuiteDoc(
    description = @Desc("Test suite for measuring User Operator scalability."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.USER_OPERATOR)
    }
)
@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class UserOperatorScalabilityPerformance extends AbstractST {

    protected static final String REPORT_DIRECTORY = "user-operator";

    protected UserOperatorPerformanceReporter userOperatorPerformanceReporter = new UserOperatorPerformanceReporter();

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorScalabilityPerformance.class);

    @TestDoc(
        description = @Desc("This test measures throughput (time to process N users in parallel), NOT latency (response time for a single user)."),
        steps = {
            @Step(value = "Deploy Kafka cluster with User Operator configured with more resources to handle load.", expected = "Kafka cluster with User Operator is deployed and ready."),
            @Step(value = "For each configured number of users (10, 100, 200, 500), spawn one thread per KafkaUser to perform its full lifecycle concurrently.", expected = "N concurrent threads are created, each responsible for one KafkaUser full lifecycle (create, modify, delete)."),
            @Step(value = "Each thread performs CREATE: Creates KafkaUser with TLS authentication and ACL authorization.", expected = "KafkaUser is created and ready."),
            @Step(value = "Each thread performs MODIFY: Updates ACL rules and adds quotas.", expected = "KafkaUser is updated and reconciled."),
            @Step(value = "Each thread performs DELETE: Deletes the KafkaUser.", expected = "KafkaUser and associated Secret are deleted."),
            @Step(value = "Wait for all threads to complete their full lifecycle operations and measure total elapsed time.", expected = "All KafkaUsers have completed create-modify-delete lifecycle. Total time represents THROUGHPUT capacity (time for all N users to complete), not individual user LATENCY."),
            @Step(value = "Clean up any remaining users and collect performance metrics (e.g., total time to complete all user lifecycles) i.e., reconciliation time.", expected = "Namespace is cleaned, performance data is persisted to user-operator report directory for analysis.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @IsolatedTest
    void testScalability() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final List<Integer> numberOfKafkaUsersToTest = List.of(10, 100, 200, 500);
        // default configuration of UO
        final int maxBatchSize = 100;
        final int maxBatchLingerMs = 100;
        final int maxWorkQueueSize = 1024;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(),  testStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .endKafka()
                        .editEntityOperator()
                            .editUserOperator()
                                .withReconciliationIntervalMs(10_000L)
//                                .withResources(new ResourceRequirementsBuilder()
//                                    .addToLimits("memory", new Quantity("768Mi"))
//                                    .addToLimits("cpu", new Quantity("750m"))
//                                    .addToRequests("memory", new Quantity("768Mi"))
//                                    .addToRequests("cpu", new Quantity("750m"))
//                                    .build())
                            .endUserOperator()
                            .editOrNewTemplate()
                                .editOrNewUserOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_WORK_QUEUE_SIZE")
                                        .withValue(String.valueOf(maxWorkQueueSize))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE")
                                        .withValue(String.valueOf(maxBatchSize))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS")
                                        .withValue(String.valueOf(maxBatchLingerMs))
                                    .endEnv()
                                .endUserOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                    .endSpec()
                .build()
        );

        numberOfKafkaUsersToTest.forEach(numberOfKafkaUsers -> {
            long reconciliationTimeMs = 0;
            try {
                // number of KafkaUsers to test (each goes through full lifecycle: create, modify, delete)
                reconciliationTimeMs = UserOperatorPerformanceUtils.processAllUsersConcurrently(testStorage, numberOfKafkaUsers, 0, 0);
            } finally {
                LOGGER.info("Cleaning namespace: {}", testStorage.getNamespaceName());
                List<KafkaUser> kafkaUsers = CrdClients.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).list().getItems();
                KubeResourceManager.get().deleteResourceAsyncWait(kafkaUsers.toArray(new KafkaUser[0]));
                KafkaUserUtils.waitForUserWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getUsername());

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();

                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, maxWorkQueueSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, maxBatchSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, numberOfKafkaUsers);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, maxBatchLingerMs);

                performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL, reconciliationTimeMs);

                try {
                    this.userOperatorPerformanceReporter.logPerformanceData(testStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE, TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @TestDoc(
        description = @Desc("This test measures user modification latency statistics under different load levels by performing multiple user modifications to understand how response time scales with system load."),
        steps = {
            @Step(value = "Deploy Kafka cluster with User Operator configured with more resources to handle load and also non-default `STRIMZI_WORK_QUEUE_SIZE` set to 2048.", expected = "Kafka cluster with User Operator is deployed and ready."),
            @Step(value = "For each configured load level (1000, 1500, 2000 existing users), create N KafkaUsers to establish the load.", expected = "N KafkaUsers are created and ready, establishing baseline load on the User Operator."),
            @Step(value = "Perform 100 individual user modifications sequentially, measuring the latency of each modification.", expected = "Each modification latency is recorded independently."),
            @Step(value = "Calculate latency statistics: min, max, average, P50, P95, and P99 percentiles from the 100 measurements.", expected = "Statistical analysis shows how single-user modification latency degrades as system load (number of existing users) increases."),
            @Step(value = "Clean up all users and persist latency metrics to user-operator report directory.", expected = "Namespace is cleaned, latency data is saved showing how responsiveness changes at different load levels.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @IsolatedTest
    @Tag(SCALABILITY)
    void testLatencyUnderLoad() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final List<Integer> loadLevels = List.of(110, 200, 300);
        final int numberOfModifications = 100;
        // default configuration of UO
        final int maxBatchSize = 100;
        final int maxBatchLingerMs = 100;
        // but maxWorkQueueSize must be a bit higher than default because we Queue will be `FULL`
        final int maxWorkQueueSize = 2048;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(),  testStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .endKafka()
                        .editEntityOperator()
                            .editUserOperator()
                                .withReconciliationIntervalMs(10_000L)
//                                .withResources(new ResourceRequirementsBuilder()
//                                    .addToLimits("memory", new Quantity("768Mi"))
//                                    .addToLimits("cpu", new Quantity("750m"))
//                                    .addToRequests("memory", new Quantity("768Mi"))
//                                    .addToRequests("cpu", new Quantity("750m"))
//                                    .build())
                            .endUserOperator()
                            .editOrNewTemplate()
                                .editOrNewUserOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_WORK_QUEUE_SIZE")
                                        .withValue(String.valueOf(maxWorkQueueSize))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE")
                                        .withValue(String.valueOf(maxBatchSize))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS")
                                        .withValue(String.valueOf(maxBatchLingerMs))
                                    .endEnv()
                                .endUserOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                    .endSpec()
                .build()
        );

        loadLevels.forEach(numberOfExistingUsers -> {
            LatencyMetrics latencyMetrics = null;
            try {
                LOGGER.info("Measuring single-user modification latency with {} existing users in the system", numberOfExistingUsers);
                latencyMetrics = UserOperatorPerformanceUtils.measureLatencyUnderLoad(testStorage, numberOfExistingUsers, numberOfModifications);
            } finally {
                LOGGER.info("Cleaning namespace: {}", testStorage.getNamespaceName());
                List<KafkaUser> kafkaUsers = CrdClients.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).list().getItems();
                KubeResourceManager.get().deleteResourceAsyncWait(kafkaUsers.toArray(new KafkaUser[0]));
                KafkaUserUtils.waitForUserWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getUsername());

                if (latencyMetrics != null) {
                    final Map<String, Object> performanceAttributes = new LinkedHashMap<>();

                    // Input parameters
                    performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, maxWorkQueueSize);
                    performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, maxBatchSize);
                    performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, numberOfExistingUsers);
                    performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, maxBatchLingerMs);

                    // Latency metrics
                    performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_MIN_LATENCY, latencyMetrics.min());
                    performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_MAX_LATENCY, latencyMetrics.max());
                    performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_AVERAGE_LATENCY, latencyMetrics.average());
                    performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_P50_LATENCY, latencyMetrics.p50());
                    performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_P95_LATENCY, latencyMetrics.p95());
                    performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_P99_LATENCY, latencyMetrics.p99());

                    try {
                        this.userOperatorPerformanceReporter.logPerformanceData(testStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_LATENCY_USE_CASE, TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();
    }

    @AfterAll
    void tearDown() {
        // show tables with metrics
        UserOperatorMetricsParser.main(new String[]{PerformanceConstants.USER_OPERATOR_PARSER});
    }
}

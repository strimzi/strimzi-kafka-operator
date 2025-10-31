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
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.performance.report.UserOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.UserOperatorMetricsParser;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils;
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
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@SuiteDoc(
    description = @Desc("Test suite for measuring User Operator scalability."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
        @Step(value = "Deploy Kafka cluster with User Operator configured with more resources to handle load.", expected = "Kafka cluster with User Operator is deployed and ready.")
    },
    labels = {
        @Label(TestDocsLabels.USER_OPERATOR)
    }
)
@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class UserOperatorScalabilityPerformance extends AbstractST {

    protected static final TemporalAccessor ACTUAL_TIME = LocalDateTime.now();
    protected static final String REPORT_DIRECTORY = "user-operator";

    protected UserOperatorPerformanceReporter userOperatorPerformanceReporter = new UserOperatorPerformanceReporter();

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorScalabilityPerformance.class);

    private TestStorage suiteTestStorage;

    // user event batches to test
    private final List<Integer> eventBatches = List.of(70, 700, 1400, 3500);
    // default configuration of UO
    private final int maxBatchSize = 100;
    private final int maxBatchLingerMs = 100;
    private final int maxQueueSize = 10_000;
    private long reconciliationTimeMs;

    @TestDoc(
        description = @Desc("Verifies User Operator scalability by processing concurrent KafkaUser operations with varying event batch sizes " +
            "(70, 700, 1400, 3500 events), measuring reconciliation time and collecting performance metrics. " +
            "What is worth of nothing is that KafkaUser operation generates multiple Kubernetes watch events: " +
            " (i.) CREATE triggers 3 events (KafkaUser ADDED + Secret ADDED + KafkaUser status MODIFIED), " +
            " (ii.) MODIFY triggers 2 events (KafkaUser MODIFIED + KafkaUser status MODIFIED), " +
            " (iii.) DELETE triggers 2 events (KafkaUser DELETED + Secret DELETED). " +
            "With 7 events per task, batch sizes correspond to 10, 100, 200, and 500 KafkaUsers."),
        steps = {
            @Step(value = "For each batch size (70, 700, 1400, 3500 events = 10, 100, 200, 500 users), spawn a separate thread for each KafkaUser to perform its full lifecycle (create, modify, delete) concurrently.", expected = "Each thread creates a KafkaUser with TLS authentication and ACL authorization rules, waits for it to be ready, modifies it, waits for reconciliation, then deletes it."),
            @Step(value = "Wait for all threads to complete their KafkaUser lifecycle operations.", expected = "All KafkaUsers have been created, modified, deleted, and their associated Secrets are removed."),
            @Step(value = "Measure the total reconciliation time for the batch.", expected = "Reconciliation time is recorded for performance analysis."),
            @Step(value = "Collect and log performance metrics including work queue size, batch configuration, number of users, and reconciliation time.", expected = "Performance data is persisted to the report directory for analysis.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @IsolatedTest
    void testScalability() {
        eventBatches.forEach(numEvents -> {
            final int eventPerTask = 7;
            final int numberOfTasks = numEvents / eventPerTask;
            final int numSpareEvents = numEvents % eventPerTask;
            try {
                this.reconciliationTimeMs = UserOperatorPerformanceUtils.processAllUsersConcurrently(suiteTestStorage, numberOfTasks, numSpareEvents, 0);
            } finally {
                LOGGER.info("Cleaning namespace: {}", suiteTestStorage.getNamespaceName());
                List<KafkaUser> kafkaUsers = CrdClients.kafkaUserClient().inNamespace(suiteTestStorage.getNamespaceName()).list().getItems();
                KubeResourceManager.get().deleteResourceAsyncWait(kafkaUsers.toArray(new KafkaUser[0]));
                KafkaUserUtils.waitForUserWithPrefixDeletion(suiteTestStorage.getNamespaceName(), suiteTestStorage.getUsername());

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();

                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, maxQueueSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, maxBatchSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, numberOfTasks);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, maxBatchLingerMs);

                performanceAttributes.put(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL, reconciliationTimeMs);

                try {
                    this.userOperatorPerformanceReporter.logPerformanceData(this.suiteTestStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();

        suiteTestStorage = new TestStorage(KubeResourceManager.get().getTestContext(), TestConstants.CO_NAMESPACE);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(),  suiteTestStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .endKafka()
                        .editEntityOperator()
                            .editUserOperator()
                                .withReconciliationIntervalMs(10_000L)
                                .withResources(new ResourceRequirementsBuilder()
                                    .addToLimits("memory", new Quantity("768Mi"))
                                    .addToLimits("cpu", new Quantity("750m"))
                                    .addToRequests("memory", new Quantity("768Mi"))
                                    .addToRequests("cpu", new Quantity("750m"))
                                    .build())
                            .endUserOperator()
                            .editOrNewTemplate()
                                .editOrNewUserOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_WORK_QUEUE_SIZE")
                                        .withValue(String.valueOf(maxQueueSize))
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
    }

    @AfterAll
    void tearDown() {
        // show tables with metrics
        UserOperatorMetricsParser.main(new String[]{PerformanceConstants.USER_OPERATOR_PARSER});
    }
}

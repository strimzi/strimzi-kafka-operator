/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.METRICS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricCountHigherThan;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValue;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueCount;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(SANITY)
@Tag(REGRESSION)
@Tag(METRICS)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test suite is designed for testing metrics exposed by the Strimzi Metrics Reporter."),
    beforeTestSteps = {
        @Step(value = "Create namespace {@namespace}.", expected = "Namespace {@namespace} is created."),
        @Step(value = "Deploy Cluster Operator.", expected = "Cluster Operator is deployed."),
        @Step(value = "Deploy Kafka {@clusterName} with metrics.", expected = "Kafka @{clusterName} is deployed."),
        @Step(value = "Deploy scraper Pod in namespace {@namespace} for collecting metrics from Strimzi pods.", expected = "Scraper Pods is deployed."),
        @Step(value = "Create KafkaTopic resource.", expected = "KafkaTopic resource is Ready."),
        @Step(value = "Create collector for Kafka.", expected = "Metrics collected in collectors structs.")
    },
    afterTestSteps = {
        @Step(value = "Common cleaning of all resources created by this test class.", expected = "All resources deleted.")
    },
    labels = {
        @Label(value = TestDocsLabels.METRICS),
    }
)
public class StrimziMetricsReporterST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(StrimziMetricsReporterST.class);

    private final String namespace = Environment.TEST_SUITE_NAMESPACE;
    private final String clusterName = "my-cluster";

    private BaseMetricsCollector kafkaCollector;

    @ParallelTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by Kafka."),
        steps = {
            @Step(value = "Check if specific metrics are available in collected metrics from Kafka Pods.", expected = "Metrics are available with expected values.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testKafkaMetrics() {
        assertMetricValueCount(kafkaCollector, "kafka_server_replicamanager_leadercount", 3.0);
        assertMetricCountHigherThan(kafkaCollector, "kafka_server_replicamanager_partitioncount", 2);
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_underreplicatedpartitions", 0.0);
    }

    @BeforeAll
    void setupEnvironment() {
        // metrics tests are not designed to run with namespace RBAC scope
        assumeFalse(Environment.isNamespaceRbacScope());

        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        cluster.setNamespace(namespace);

        String scraperName = namespace + "-" + TestConstants.SCRAPER_NAME;

        // create resources without wait to deploy them simultaneously
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(namespace, KafkaNodePoolResource.getBrokerPoolName(clusterName), clusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(namespace, KafkaNodePoolResource.getControllerPoolName(clusterName), clusterName, 3).build()
        );
        resourceManager.createResourceWithoutWait(
            KafkaTemplates.kafkaWithStrimziMetricsReporter(namespace, clusterName, 3).build(),
            ScraperTemplates.scraperPod(namespace, scraperName).build()
        );

        // sync resources
        resourceManager.synchronizeResources();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(namespace, KafkaTopicUtils.generateRandomNameOfTopic(), clusterName, 5, 2).build());

        String scraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(namespace, scraperName).get(0).getMetadata().getName();

        // wait some time for metrics to be stable, at least reconciliation interval + 10s
        LOGGER.info("Sleeping for {} to give operators and operands some time to stable the metrics values before collecting",
                TestConstants.SAFETY_RECONCILIATION_INTERVAL);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(TestConstants.SAFETY_RECONCILIATION_INTERVAL));

        kafkaCollector = new BaseMetricsCollector.Builder()
            .withScraperPodName(scraperPodName)
            .withNamespaceName(namespace)
            .withComponent(KafkaMetricsComponent.create(clusterName))
            .build();

        kafkaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
    }
}

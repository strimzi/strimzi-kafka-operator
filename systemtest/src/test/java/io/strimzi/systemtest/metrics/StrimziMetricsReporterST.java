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
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.METRICS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValue;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueHigherThanOrEqualTo;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(SANITY)
@Tag(REGRESSION)
@Tag(METRICS)
@SuiteDoc(
    description = @Desc("This test suite is designed for testing metrics exposed by the Strimzi Metrics Reporter."),
    beforeTestSteps = {
        @Step(value = "Create namespace {@namespace}.", expected = "Namespace {@namespace} is created."),
        @Step(value = "Deploy Cluster Operator.", expected = "Cluster Operator is deployed."),
        @Step(value = "Deploy Kafka {@clusterName} with Strimzi Metrics Reporter.", expected = "Kafka @{clusterName} is deployed."),
        @Step(value = "Deploy scraper Pod in namespace {@namespace} for collecting metrics from Strimzi pods.", expected = "Scraper Pod is deployed."),
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

    private static final int BROKER_REPLICAS = 3;
    private static final int TARGET_BROKER_REPLICAS = 3;
    private static final String CONNECT_CLUSTER_NAME = "my-connect";
    private static final String MM2_CLUSTER_NAME = "my-mm2";

    private TestStorage testStorage;
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
        kafkaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricValueHigherThanOrEqualTo(kafkaCollector, "kafka_server_replicamanager_leadercount", 1.0);
        assertMetricValueHigherThanOrEqualTo(kafkaCollector, "kafka_server_replicamanager_partitioncount", 1.0);
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_underreplicatedpartitions", 0.0);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several random metrics exposed by Kafka Connect."),
        steps = {
            @Step(value = "Deploy KafkaConnect into {@namespace}.", expected = "KafkaConnect is up and running."),
            @Step(value = "Create KafkaConnector for KafkaConnect from step 1.", expected = "KafkaConnector is in Ready state."),
            @Step(value = "Create metrics collector and collect metrics from KafkaConnect Pods.", expected = "Metrics are collected."),
            @Step(value = "Check if specific metric is available in collected metrics from KafkaConnect Pods.", expected = "Metric is available with expected value.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testKafkaConnectAndConnectorMetrics() {
        KubeResourceManager.get().createResourceWithWait(
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), CONNECT_CLUSTER_NAME,
                            testStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .withNewStrimziMetricsReporterConfig()
                        .withNewValues()
                            .withAllowList("kafka_connect.*")
                        .endValues()
                    .endStrimziMetricsReporterConfig()
                .endSpec().build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), CONNECT_CLUSTER_NAME).build()
        );

        BaseMetricsCollector kafkaConnectCollector = kafkaCollector.toBuilder()
            .withComponent(KafkaConnectMetricsComponent.create(CONNECT_CLUSTER_NAME))
            .build();

        kafkaConnectCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricValueHigherThanOrEqualTo(kafkaConnectCollector, "kafka_connect_connect_node_metrics_request\\{client_id=\".*\"}", 0.0);
        assertMetricValueHigherThanOrEqualTo(kafkaConnectCollector, "kafka_connect_connect_node_metrics_response\\{client_id=\".*\"}", 0.0);
        assertMetricValueHigherThanOrEqualTo(kafkaConnectCollector, "kafka_connect_connect_metrics_network_io\\{client_id=\".*\"}", 0.0);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by KafkaMirrorMaker2."),
        steps = {
            @Step(value = "Deploy KafkaMirrorMaker2 into {@namespace}.", expected = "KafkaMirrorMaker2 is in Ready state."),
            @Step(value = "Collect metrics from KafkaMirrorMaker2 pod.", expected = "Metrics are collected."),
            @Step(value = "Check if specific metric is available in collected metrics from KafkaMirrorMaker2 pods.", expected = "Metric is available with expected value.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.MIRROR_MAKER_2)
        }
    )
    void testMirrorMaker2Metrics() {
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.mixedPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(),
                    testStorage.getTargetClusterName(), TARGET_BROKER_REPLICAS).build()
        );
        
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), TARGET_BROKER_REPLICAS)
                .editSpec()
                    .editEntityOperator()
                        .withTopicOperator(null)
                        .withUserOperator(null)
                    .endEntityOperator()
                .endSpec().build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), MM2_CLUSTER_NAME,
                    testStorage.getClusterName(), testStorage.getTargetClusterName(), 1, false)
                .editOrNewSpec()
                    .withNewStrimziMetricsReporterConfig()
                        .withNewValues()
                            .withAllowList("kafka_connect.*")
                        .endValues()
                    .endStrimziMetricsReporterConfig()
                .endSpec().build()
        );

        BaseMetricsCollector kmm2Collector = kafkaCollector.toBuilder()
            .withComponent(KafkaMirrorMaker2MetricsComponent.create(MM2_CLUSTER_NAME))
            .build();

        kmm2Collector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricValue(kmm2Collector, "kafka_connect_connect_worker_metrics_connector_count", 2.0);
        assertMetricValueHigherThanOrEqualTo(kmm2Collector, "kafka_connect_connect_worker_metrics_task_count", 0.0);
    }

    @BeforeAll
    void setupEnvironment() {
        testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // metrics tests are not designed to run with namespace RBAC scope
        assumeFalse(Environment.isNamespaceRbacScope());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        cluster.setNamespace(testStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(),
                    testStorage.getClusterName(), BROKER_REPLICAS).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(),
                    testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), BROKER_REPLICAS)
                .editSpec()
                    .editKafka()
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_server.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec().build(),
            KafkaTemplates.kafkaWithoutEntityOperator(testStorage.getNamespaceName(), testStorage.getClusterName() + "-tgt", 1).build()
        );

        KubeResourceManager.get().createResourceWithoutWait(
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(),
                    testStorage.getClusterName(), 5, BROKER_REPLICAS).build()
        );

        // wait some time for metrics to be stable, at least reconciliation interval + 10s
        LOGGER.info("Sleeping for {} to give operators and operands some time to stable the metrics values before collecting",
                TestConstants.SAFETY_RECONCILIATION_INTERVAL);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(TestConstants.SAFETY_RECONCILIATION_INTERVAL));

        kafkaCollector = new BaseMetricsCollector.Builder()
                .withScraperPodName(KubeResourceManager.get().kubeClient()
                        .listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName())
                .withNamespaceName(testStorage.getNamespaceName())
                .withComponent(KafkaMetricsComponent.create(testStorage.getClusterName()))
                .build();
    }
}

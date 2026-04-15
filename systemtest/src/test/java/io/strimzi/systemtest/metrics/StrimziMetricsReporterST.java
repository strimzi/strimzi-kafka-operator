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
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.testclients.clients.http.HttpProducerConsumer;
import io.strimzi.testclients.clients.http.HttpProducerConsumerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.METRICS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValue;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueHigherThanOrEqualTo;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
    private static final String BRIDGE_NAME = "my-bridge";

    private TestStorage suiteTestStorage;
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
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(suiteTestStorage.getNamespaceName(), CONNECT_CLUSTER_NAME,
                            suiteTestStorage.getClusterName(), 1)
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
                KafkaConnectorTemplates.kafkaConnector(suiteTestStorage.getNamespaceName(), CONNECT_CLUSTER_NAME).build()
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
            KafkaNodePoolTemplates.mixedPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getTargetBrokerPoolName(),
                    suiteTestStorage.getTargetClusterName(), TARGET_BROKER_REPLICAS).build()
        );
        
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getTargetClusterName(), TARGET_BROKER_REPLICAS)
                .editSpec()
                    .editEntityOperator()
                        .withTopicOperator(null)
                        .withUserOperator(null)
                    .endEntityOperator()
                .endSpec().build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(suiteTestStorage.getNamespaceName(), MM2_CLUSTER_NAME,
                    suiteTestStorage.getClusterName(), suiteTestStorage.getTargetClusterName(), 1, false)
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

    @ParallelTest
    @Tag(BRIDGE)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by KafkaBridge."),
        steps = {
            @Step(value = "Deploy KafkaBridge into {@namespace}.", expected = "KafkaBridge is deployed and Ready"),
            @Step(value = "Attach producer and consumer clients to KafkaBridge", expected = "Clients are up and running, continuously producing and pooling messages"),
            @Step(value = "Collect metrics from KafkaBridge pod", expected = "Metrics are collected"),
            @Step(value = "Check that specific metric is available in collected metrics from KafkaBridge pods", expected = "Metric is available with expected value")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.BRIDGE)
        }
    )
    void testKafkaBridgeMetrics() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getNamespaceName(), BRIDGE_NAME,
                    KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
                .editSpec()
                    .withNewStrimziMetricsReporterConfig()
                        .withNewValues()
                            .withAllowList("kafka.*,strimzi_bridge.*")
                        .endValues()
                    .endStrimziMetricsReporterConfig()
                .endSpec().build()
        );

        // allow connections from scraper to Bridge pods when NetworkPolicies are set to denied by default
        NetworkPolicyUtils.allowNetworkPolicySettingsForBridgeScraper(suiteTestStorage.getNamespaceName(),
            suiteTestStorage.getScraperName(), KafkaBridgeResources.componentName(BRIDGE_NAME));

        // Create NetworkPolicies for HTTP clients to access Bridge
        NetworkPolicyUtils.allowNetworkPoliciesForBridgeClients(suiteTestStorage.getNamespaceName(), BRIDGE_NAME, testStorage.getProducerName(), testStorage.getConsumerName());

        // Attach consumer before producer
        HttpProducerConsumer httpProducerConsumer = new HttpProducerConsumerBuilder()
            .withNamespaceName(suiteTestStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withHostname(KafkaBridgeResources.serviceName(BRIDGE_NAME))
            .withTopicName(suiteTestStorage.getTopicName())
            .withMessageCount(suiteTestStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(200)
            .build();

        // we cannot wait for producer and consumer to complete to see all needed metrics - especially `strimzi_bridge_kafka_producer_count`
        KubeResourceManager.get().createResourceWithWait(
            httpProducerConsumer.getProducer().getJob(),
            httpProducerConsumer.getConsumer().getJob()
        );

        BaseMetricsCollector bridgeCollector = kafkaCollector.toBuilder()
            .withComponent(KafkaBridgeMetricsComponent.create(suiteTestStorage.getNamespaceName(), BRIDGE_NAME))
            .build();

        bridgeCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricValueNotNull(bridgeCollector, "kafka_producer_kafka_metrics_count_count\\{.*}");
        assertMetricValueNotNull(bridgeCollector, "kafka_consumer_consumer_metrics_connection_count\\{.*}");
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test checking that `prometheus.metrics.reporter.allowlist` configuration is dynamically updatable using the .spec.kafka.metricsConfig.allowList."),
        steps = {
            @Step(value = "Check that `kafka_server_replicamanager_leadercount` is present and `kafka_log_log_logendoffset` not - in already deployed Kafka cluster", expected = "`kafka_server_replicamanager_leadercount` is present and `kafka_log_log_logendoffset` not."),
            @Step(value = "In already deployed Kafka cluster, change the configuration of the allowList to have just `kafka_log.*` metrics allowed.", expected = "The configuration of allowList is changed."),
            @Step(value = "Wait some time to verify that there will be no rolling update because of the change - verification of dynamic reconfiguration.", expected = "No Pods were rolled."),
            @Step(value = "Collect metrics and check that there is no metric like `kafka_server_replicamanager_leadercount` (because of removal of `kafka_server.*` metrics from allowList).", expected = "No metric has been found."),
            @Step(value = "Check that collected metrics contain `kafka_log_log_logstartoffset` metric for the `__cluster_metadata` topic.", expected = "Metric is present."),
            @Step(value = "Change the allowList back to previous state - allowing just `kafka_server.*`", expected = "The configuration of allowList is changed."),
            @Step(value = "Wait some time to verify that there will be no rolling update because of the change - verification of dynamic reconfiguration.", expected = "No Pods were rolled."),
            @Step(value = "Check that `kafka_server_replicamanager_leadercount` is present and `kafka_log_log_logendoffset` not - configuration change was successful", expected = "`kafka_server_replicamanager_leadercount` is present and `kafka_log_log_logendoffset` not."),
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION)
        }
    )
    void testDynamicReconfigurationAllowList() {
        LOGGER.info("Checking that `kafka_server_replicamanager_leadercount` metrics contains some value and `kafka_log_log_logendoffset` doesn't exist");
        // Firstly check that before the change, there is `kafka_server_replicamanager_leadercount` metric present
        assertMetricValueHigherThanOrEqualTo(kafkaCollector, "kafka_server_replicamanager_leadercount", 1.0);
        // And check that `kafka_log_log_logendoffset` is not present
        assertThat(MetricsUtils.createPatternAndCollectWithoutWait(kafkaCollector, "kafka_log_log_logendoffset").isEmpty(), is(true));

        Map<String, String> kafkaPods = PodUtils.podSnapshot(suiteTestStorage.getNamespaceName(), LabelSelectors.allKafkaPodsLabelSelector(suiteTestStorage.getClusterName()));

        // change configuration to disable kafka_server.* metrics and add kafka_controller.* metrics
        LOGGER.info("Change the allowList to allow `kafka_log.*`.");
        KafkaUtils.replace(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), kafka -> {
                StrimziMetricsReporter config = (StrimziMetricsReporter) kafka.getSpec().getKafka().getMetricsConfig();
                config.getValues().setAllowList(List.of("kafka_log.*"));

                kafka.getSpec().getKafka().setMetricsConfig(config);
            }
        );

        RollingUpdateUtils.waitForNoRollingUpdate(suiteTestStorage.getNamespaceName(), LabelSelectors.allKafkaPodsLabelSelector(suiteTestStorage.getClusterName()), kafkaPods);

        LOGGER.info("Check if Kafka Pods are missing the 'kafka_server_' metrics");
        kafkaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertThat(MetricsUtils.createPatternAndCollectWithoutWait(kafkaCollector, "kafka_server_replicamanager_leadercount").isEmpty(), is(true));

        LOGGER.info("Now check if Kafka Pods contain 'kafka_log.*' metrics");
        assertMetricValueNotNull(kafkaCollector, "kafka_log_log_logstartoffset\\{partition=\"0\",topic=\"__cluster_metadata\"\\}");

        LOGGER.info("Changing back to previous state - allowing `kafka_server.*` metrics");
        KafkaUtils.replace(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), kafka -> {
                StrimziMetricsReporter config = (StrimziMetricsReporter) kafka.getSpec().getKafka().getMetricsConfig();
                config.getValues().setAllowList(List.of("kafka_server.*"));

                kafka.getSpec().getKafka().setMetricsConfig(config);
            }
        );

        RollingUpdateUtils.waitForNoRollingUpdate(suiteTestStorage.getNamespaceName(), LabelSelectors.allKafkaPodsLabelSelector(suiteTestStorage.getClusterName()), kafkaPods);

        LOGGER.info("Check if Kafka Pods are not missing the 'kafka_server_' metrics");
        kafkaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricValueHigherThanOrEqualTo(kafkaCollector, "kafka_server_replicamanager_leadercount", 1.0);
    }

    @BeforeAll
    void setupEnvironment() {
        suiteTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // metrics tests are not designed to run with namespace RBAC scope
        assumeFalse(Environment.isNamespaceRbacScope());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        cluster.setNamespace(suiteTestStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(),
                    suiteTestStorage.getClusterName(), BROKER_REPLICAS).build(),
            KafkaNodePoolTemplates.controllerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(),
                    suiteTestStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), BROKER_REPLICAS)
                .editSpec()
                    .editKafka()
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_server.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec().build()
        );

        KubeResourceManager.get().createResourceWithoutWait(
            ScraperTemplates.scraperPod(suiteTestStorage.getNamespaceName(), suiteTestStorage.getScraperName()).build(),
            KafkaTopicTemplates.topic(suiteTestStorage.getNamespaceName(), suiteTestStorage.getTopicName(),
                    suiteTestStorage.getClusterName(), 5, BROKER_REPLICAS).build()
        );

        // wait some time for metrics to be stable, at least reconciliation interval + 10s
        LOGGER.info("Sleeping for {} to give operators and operands some time to stable the metrics values before collecting",
                TestConstants.SAFETY_RECONCILIATION_INTERVAL);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(TestConstants.SAFETY_RECONCILIATION_INTERVAL));

        kafkaCollector = new BaseMetricsCollector.Builder()
                .withScraperPodName(KubeResourceManager.get().kubeClient()
                        .listPodsByPrefixInName(suiteTestStorage.getNamespaceName(), suiteTestStorage.getScraperName()).get(0).getMetadata().getName())
                .withNamespaceName(suiteTestStorage.getNamespaceName())
                .withComponent(KafkaMetricsComponent.create(suiteTestStorage.getClusterName()))
                .build();
    }
}

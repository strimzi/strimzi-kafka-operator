/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.logs.CollectorElement;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.METRICS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourceState;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourceStateNotExists;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResources;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourcesNullOrZero;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricCountHigherThan;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResources;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValue;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueCount;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueHigherThan;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueNullOrZero;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.getExporterRunScript;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.getResourceMetricPattern;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(SANITY)
@Tag(REGRESSION)
@Tag(METRICS)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test suite is designed for testing metrics exposed by operators and operands."),
    beforeTestSteps = {
        @Step(value = "Create namespaces {@namespaceFirst} and {@namespaceSecond}.", expected = "Namespaces {@namespaceFirst} and {@namespaceSecond} are created."),
        @Step(value = "Deploy Cluster Operator.", expected = "Cluster Operator is deployed."),
        @Step(value = "Deploy Kafka {@kafkaClusterFirstName} with metrics and CruiseControl configured.", expected = "Kafka @{kafkaClusterFirstName} is deployed."),
        @Step(value = "Deploy Kafka {@kafkaClusterSecondtName} with metrics configured.", expected = "Kafka @{kafkaClusterFirstName} is deployed."),
        @Step(value = "Deploy scraper Pods in namespace {@namespaceFirst} and {@namespaceSecond} for collecting metrics from Strimzi pods.", expected = "Scraper Pods are deployed."),
        @Step(value = "Create KafkaUsers and KafkaTopics.", expected = "All KafkaUsers and KafkaTopics are Ready."),
        @Step(value = "Setup NetworkPolicies to grant access to Operator Pods and KafkaExporter.", expected = "NetworkPolicies created."),
        @Step(value = "Create collectors for Cluster Operator, Kafka, and KafkaExporter.", expected = "Metrics collected in collectors structs.")
    },
    afterTestSteps = {
        @Step(value = "Common cleaning of all resources created by this test class.", expected = "All resources deleted.")
    },
    labels = {
        @Label(value = TestDocsLabels.METRICS),
    }
)
public class MetricsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);

    private final String namespaceFirst = "metrics-test-0";
    private final String namespaceSecond = "metrics-test-1";
    private final String kafkaClusterFirstName = "metrics-cluster-0";
    private final String kafkaClusterSecondName = "metrics-cluster-1";
    private final String mm2ClusterName = "mm2-cluster";
    private final String bridgeClusterName = "my-bridge";

    private String coScraperPodName;
    private String scraperPodName;

    private String bridgeTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private final String kafkaExporterTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

    private BaseMetricsCollector kafkaCollector;
    private BaseMetricsCollector kafkaExporterCollector;
    private BaseMetricsCollector clusterOperatorCollector;

    @ParallelTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by Kafka."),
        steps = {
            @Step(value = "Check if specific metric is available in collected metrics from Kafka Pods.", expected = "Metric is available with expected value.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testKafkaMetrics() {
        assertMetricValueCount(kafkaCollector, "kafka_server_replicamanager_leadercount", 3);
        assertMetricCountHigherThan(kafkaCollector, "kafka_server_replicamanager_partitioncount", 2);
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_underreplicatedpartitions", 0);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several random metrics exposed by Kafka Connect."),
        steps = {
            @Step(value = "Deploy KafkaConnect into {@namespaceFirst}.", expected = "KafkaConnect is up and running."),
            @Step(value = "Create KafkaConnector for KafkaConnect from step 1.", expected = "KafkaConnector is in Ready state."),
            @Step(value = "Create metrics collector and collect metrics from KafkaConnect Pods.", expected = "Metrics are collected."),
            @Step(value = "Check if specific metric is available in collected metrics from KafkaConnect Pods.", expected = "Metric is available with expected value."),
            @Step(value = "Collect current metrics from Cluster Operator Pod.", expected = "Cluster Operator metrics are collected."),
            @Step(value = "Check that CO metrics contain data about KafkaConnect and KafkaConnector in namespace {@namespaceFirst}.", expected = "CO metrics contain expected data."),
            @Step(value = "Check that CO metrics don't contain data about KafkaConnect and KafkaConnector in namespace {@namespaceSecond}.", expected = "CO metrics should not contain any data for given namespace."),
            @Step(value = "Check that CO metrics contain data about KafkaConnect state.", expected = "CO metrics contain expected data.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.CONNECT),
        }
    )
    void testKafkaConnectAndConnectorMetrics() {
        resourceManager.createResourceWithWait(
            KafkaConnectTemplates.connectMetricsConfigMap(namespaceFirst, kafkaClusterFirstName),
            KafkaConnectTemplates.kafkaConnectWithMetricsAndFileSinkPlugin(namespaceFirst, kafkaClusterFirstName, kafkaClusterFirstName, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build());
        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(namespaceFirst, kafkaClusterFirstName).build());

        BaseMetricsCollector kafkaConnectCollector = kafkaCollector.toBuilder()
            .withComponent(KafkaConnectMetricsComponent.create(kafkaClusterFirstName))
            .build();

        kafkaConnectCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_request_total\\{clientid=\".*\"}", 0);
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_response_total\\{clientid=\".*\".*}", 0);
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_network_io_total\\{clientid=\".*\".*}", 0);

        // Check CO metrics and look for KafkaConnect and KafkaConnector
        clusterOperatorCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertCoMetricResources(namespaceFirst, KafkaConnect.RESOURCE_KIND, clusterOperatorCollector, 1);
        assertCoMetricResourcesNullOrZero(namespaceSecond, KafkaConnect.RESOURCE_KIND, clusterOperatorCollector);
        assertCoMetricResourceState(namespaceFirst, KafkaConnect.RESOURCE_KIND, kafkaClusterFirstName, clusterOperatorCollector, 1, "none");

        assertCoMetricResources(namespaceFirst, KafkaConnector.RESOURCE_KIND, clusterOperatorCollector, 1);
        assertCoMetricResourcesNullOrZero(namespaceSecond, KafkaConnector.RESOURCE_KIND, clusterOperatorCollector);

        assertMetricValueHigherThan(clusterOperatorCollector, getResourceMetricPattern(namespaceFirst, StrimziPodSet.RESOURCE_KIND), 1);
        assertMetricValueHigherThan(clusterOperatorCollector, getResourceMetricPattern(namespaceSecond, StrimziPodSet.RESOURCE_KIND), 0);
    }

    @IsolatedTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by KafkaExporter."),
        steps = {
            @Step(value = "Create Kafka producer and consumer and exchange some messages.", expected = "Clients successfully exchange the messages."),
            @Step(value = "Check if metric kafka_consumergroup_current_offset is available in collected metrics from KafkaExporter Pods.", expected = "Metric is available with expected value."),
            @Step(value = "Check if metric kafka_broker_info is available in collected metrics from KafkaExporter pods for each Kafka Broker pod.", expected = "Metric is available with expected value.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testKafkaExporterMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String kafkaStrimziPodSetName = StrimziPodSetResource.getBrokerComponentName(kafkaClusterFirstName);
        final LabelSelector brokerPodsSelector = KafkaResource.getLabelSelector(kafkaClusterFirstName, kafkaStrimziPodSetName);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(kafkaExporterTopicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterFirstName))
            .withNamespaceName(namespaceFirst)
            .withMessageCount(5000)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(namespaceFirst, testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount(), false);

        assertMetricValueNotNull(kafkaExporterCollector, "kafka_consumergroup_current_offset\\{.*\\}");

        kubeClient().listPods(namespaceFirst, brokerPodsSelector).forEach(pod -> {
            String address = pod.getMetadata().getName() + "." + kafkaClusterFirstName + "-kafka-brokers." + namespaceFirst + ".svc";
            Pattern pattern = Pattern.compile("kafka_broker_info\\{address=\"" + address + ".*\",.*} ([\\d])");
            List<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s is not null", pattern), values, notNullValue());
        });
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by KafkaExporter with different from default configuration. Rolling update is performed during the test case to change KafkaExporter configuration."),
        steps = {
            @Step(value = "Get KafkaExporter run.sh script and check it has configured proper values.", expected = "Script has proper values set."),
            @Step(value = "Check that KafkaExporter metrics contains info about consumer_offset topic.", expected = "Metrics contains proper data."),
            @Step(value = "Change configuration of KafkaExporter in Kafka CR to match 'my-group.*' group regex and {@topicName} as topic name regex, than wait for KafkaExporter rolling update.", expected = "Rolling update finished."),
            @Step(value = "Get KafkaExporter run.sh script and check it has configured proper values.", expected = "Script has proper values set."),
            @Step(value = "Check that KafkaExporter metrics don't contain info about consumer_offset topic.", expected = "Metrics contains proper data (consumer_offset is not in the metrics)."),
            @Step(value = "Revert all changes in KafkaExporter configuration and wait for Rolling Update.", expected = "Rolling update finished.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testKafkaExporterDifferentSetting() throws InterruptedException, ExecutionException, IOException {
        String consumerOffsetsTopicName = "__consumer_offsets";
        LabelSelector exporterSelector = kubeClient().getDeploymentSelectors(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName));
        String runScriptContent = getExporterRunScript(namespaceFirst, kubeClient().listPods(namespaceFirst, exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\".*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\".*\""));
        // Check that metrics contains info about consumer_offsets
        assertMetricValueNotNull(kafkaExporterCollector, "kafka_topic_partitions\\{topic=\"" + consumerOffsetsTopicName + "\"}");

        Map<String, String> kafkaExporterSnapshot = DeploymentUtils.depSnapshot(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceFirst, kafkaClusterFirstName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex("my-group.*");
            k.getSpec().getKafkaExporter().setTopicRegex(topicName);
        });

        kafkaExporterSnapshot = DeploymentUtils.waitTillDepHasRolled(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(namespaceFirst, kubeClient().listPods(namespaceFirst, exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + topicName + "\""));

        // Check that metrics don't contain info about consumer_offsets
        assertMetricValueNullOrZero(kafkaExporterCollector, "kafka_topic_partitions\\{topic=\"" + consumerOffsetsTopicName + "\"}");

        LOGGER.info("Changing Topic and group regexes back to default");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceFirst, kafkaClusterFirstName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex(".*");
            k.getSpec().getKafkaExporter().setTopicRegex(".*");
        });

        DeploymentUtils.waitTillDepHasRolled(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName), 1, kafkaExporterSnapshot);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several random metrics exposed by Cluster Operator."),
        steps = {
            @Step(value = "Check that specific metrics for Kafka reconciliation are available in metrics from Cluster Operator pod.", expected = "Metric is available with expected value."),
            @Step(value = "Check that collected metrics contain data about Kafka resource.", expected = "Metric is available with expected value."),
            @Step(value = "Check that collected metrics don't contain data about KafkaRebalance resource.", expected = "Metric is not exposed.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testClusterOperatorMetrics() {
        // Expected PodSet counts per component
        int podSetCount = 2;

        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_periodical_total", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_bucket", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_successful_total", Kafka.RESOURCE_KIND);

        assertCoMetricResources(namespaceFirst, Kafka.RESOURCE_KIND, clusterOperatorCollector, 1);
        assertCoMetricResources(namespaceSecond, Kafka.RESOURCE_KIND, clusterOperatorCollector, 1);
        assertCoMetricResourceState(namespaceFirst, Kafka.RESOURCE_KIND, kafkaClusterFirstName, clusterOperatorCollector, 1, "none");
        assertCoMetricResourceState(namespaceSecond, Kafka.RESOURCE_KIND, kafkaClusterSecondName, clusterOperatorCollector, 1, "none");

        assertCoMetricResourcesNullOrZero(namespaceFirst, KafkaRebalance.RESOURCE_KIND, clusterOperatorCollector);
        assertCoMetricResourcesNullOrZero(namespaceSecond, KafkaRebalance.RESOURCE_KIND, clusterOperatorCollector);
        assertCoMetricResourceStateNotExists(kafkaClusterFirstName, KafkaRebalance.RESOURCE_KIND, namespaceFirst, clusterOperatorCollector);

        // check StrimziPodSet metrics in CO
        assertMetricCountHigherThan(clusterOperatorCollector, getResourceMetricPattern(namespaceFirst, StrimziPodSet.RESOURCE_KIND), 0);
        assertCoMetricResources(namespaceSecond, StrimziPodSet.RESOURCE_KIND, clusterOperatorCollector, podSetCount);

        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_bucket", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_already_enqueued_total", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_successful_total", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_total", StrimziPodSet.RESOURCE_KIND);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by User Operator."),
        steps = {
            @Step(value = "Collect metrics from User Operator pod.", expected = "Metrics are collected."),
            @Step(value = "Check that specific metrics about KafkaUser are available in collected metrics.", expected = "Metric is available with expected value.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testUserOperatorMetrics() {
        BaseMetricsCollector userOperatorCollector = kafkaCollector.toBuilder()
            .withComponent(UserOperatorMetricsComponent.create(namespaceFirst, kafkaClusterFirstName))
            .build();

        userOperatorCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_successful_total", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_bucket", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_periodical_total", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_total", KafkaUser.RESOURCE_KIND);

        assertMetricResources(namespaceFirst, KafkaUser.RESOURCE_KIND, userOperatorCollector, 2);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by KafkaMirrorMaker2."),
        steps = {
            @Step(value = "Deploy KafkaMirrorMaker2 into {@namespaceFirst}.", expected = "KafkaMirrorMaker2 is in Ready state."),
            @Step(value = "Collect metrics from KafkaMirrorMaker2 pod.", expected = "Metrics are collected."),
            @Step(value = "Check if specific metric is available in collected metrics from KafkaMirrorMaker2 pods.", expected = "Metric is available with expected value."),
            @Step(value = "Collect current metrics from Cluster Operator pod.", expected = "Cluster Operator metrics are collected."),
            @Step(value = "Check that CO metrics contain data about KafkaMirrorMaker2 in namespace {@namespaceFirst}.", expected = "CO metrics contain expected data.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.MIRROR_MAKER_2)
        }
    )
    void testMirrorMaker2Metrics() {
        resourceManager.createResourceWithWait(
            KafkaMirrorMaker2Templates.mirrorMaker2MetricsConfigMap(namespaceFirst, mm2ClusterName),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2WithMetrics(namespaceFirst, mm2ClusterName, kafkaClusterFirstName, kafkaClusterSecondName, 1, namespaceFirst, namespaceSecond).build()
        );

        BaseMetricsCollector kmm2Collector = kafkaCollector.toBuilder()
            .withComponent(KafkaMirrorMaker2MetricsComponent.create(mm2ClusterName))
            .build();

        assertMetricValue(kmm2Collector, "kafka_connect_worker_connector_count", 3);
        assertMetricValue(kmm2Collector, "kafka_connect_worker_task_count", 2);

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertCoMetricResources(namespaceFirst, KafkaMirrorMaker2.RESOURCE_KIND, clusterOperatorCollector, 1);
        assertCoMetricResourcesNullOrZero(namespaceSecond, KafkaMirrorMaker2.RESOURCE_KIND, clusterOperatorCollector);
        assertCoMetricResourceState(namespaceFirst, KafkaMirrorMaker2.RESOURCE_KIND, mm2ClusterName, clusterOperatorCollector, 1, "none");
        assertMetricValueHigherThan(clusterOperatorCollector, getResourceMetricPattern(namespaceFirst, StrimziPodSet.RESOURCE_KIND), 1);
    }

    @ParallelTest
    @Tag(BRIDGE)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several metrics exposed by KafkaBridge."),
        steps = {
            @Step(value = "Deploy KafkaBridge into namespaceFirst and ensure KafkaMirrorMaker2 is in Ready state", expected = "KafkaBridge is deployed and KafkaMirrorMaker2 is Ready"),
            @Step(value = "Attach producer and consumer clients to KafkaBridge", expected = "Clients are up and running"),
            @Step(value = "Collect metrics from KafkaBridge pod", expected = "Metrics are collected"),
            @Step(value = "Check that specific metric is available in collected metrics from KafkaBridge pods", expected = "Metric is available with expected value"),
            @Step(value = "Collect current metrics from Cluster Operator pod", expected = "Cluster Operator metrics are collected"),
            @Step(value = "Check that CO metrics contain data about KafkaBridge in namespace namespaceFirst", expected = "CO metrics contain expected data")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.BRIDGE)
        }
    )
    void testKafkaBridgeMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaBridgeTemplates.kafkaBridgeWithMetrics(
                namespaceFirst,
                bridgeClusterName,
                KafkaResources.plainBootstrapAddress(kafkaClusterFirstName),
                1
                ).build()
        );

        // Allow connections from scraper to Bridge pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForBridgeScraper(namespaceFirst, scraperPodName, KafkaBridgeResources.componentName(bridgeClusterName));

        BaseMetricsCollector bridgeCollector = kafkaCollector.toBuilder()
            .withComponent(KafkaBridgeMetricsComponent.create(namespaceFirst, bridgeClusterName))
            .build();

        // Attach consumer before producer
        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withNamespaceName(namespaceFirst)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(bridgeClusterName))
            .withComponentName(KafkaBridgeResources.componentName(bridgeClusterName))
            .withTopicName(bridgeTopicName)
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(200)
            .withPollInterval(200)
            .build();

        // we cannot wait for producer and consumer to complete to see all needed metrics - especially `strimzi_bridge_kafka_producer_count`
        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge(), kafkaBridgeClientJob.consumerStrimziBridge());

        bridgeCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_producer_count\\{.*}");
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_consumer_connection_count\\{.*}");
        assertThat("bridge collected data don't contain strimzi_bridge_http_server", bridgeCollector.getCollectedData().values().toString().contains("strimzi_bridge_http_server"));

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertCoMetricResources(namespaceFirst, KafkaBridge.RESOURCE_KIND, clusterOperatorCollector, 1);
        assertCoMetricResourcesNullOrZero(namespaceSecond, KafkaBridge.RESOURCE_KIND, clusterOperatorCollector);
        assertCoMetricResourceState(namespaceFirst, KafkaBridge.RESOURCE_KIND, bridgeClusterName, clusterOperatorCollector, 1, "none");
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("This test case checks several random metrics exposed by CruiseControl."),
        steps = {
            @Step(value = "Check if specific metric is available in collected metrics from CruiseControl pods", expected = "Metric is available with expected value")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
            @Label(value = TestDocsLabels.CRUISE_CONTROL)
        }
    )
    void testCruiseControlMetrics() {
        String cruiseControlMetrics = CruiseControlUtils.callApiWithAdminCredentials(namespaceFirst, CruiseControlUtils.HttpMethod.GET, CruiseControlUtils.Scheme.HTTP,
                CruiseControlUtils.CRUISE_CONTROL_METRICS_PORT, "/metrics", "").getResponseText();
        Matcher regex = Pattern.compile("^([^#].*)\\s+([^\\s]*)$", Pattern.MULTILINE).matcher(cruiseControlMetrics);

        LOGGER.info("Verifying that we have more than 0 groups");

        assertThat(regex.groupCount(), greaterThan(0));

        while (regex.find()) {

            String metricKey = regex.group(1);
            Object metricValue = regex.group(2);

            LOGGER.debug("{} -> {}", metricKey, metricValue);

            assertThat(metricKey, not(nullValue()));
            assertThat(metricValue, not(nullValue()));
        }
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("This test case checks that the Cluster Operator propagates changes from metrics configuration done in Kafka CR into corresponding config maps."),
        steps = {
            @Step(value = "Create config map with external metrics configuration.", expected = "Config map created."),
            @Step(value = "Set ConfigMap reference from step 1 into Kafka CR and wait for pod stabilization (CO shouldn't trigger rolling update).", expected = "Wait for Kafka pods stability (60 seconds without rolling update in the row)."),
            @Step(value = "Check that metrics config maps for each pod contains data from external metrics config map.", expected = "All config maps contains proper values."),
            @Step(value = "Change config in external metrics config map.", expected = "Config map changed."),
            @Step(value = "Wait for Kafka pods stabilization (CO shouldn't trigger rolling update).", expected = "Wait for Kafka pods stability (60 seconds without rolling update in the row)."),
            @Step(value = "Check that metrics config maps for each pod contains data from external metrics config map.", expected = "All config maps contains proper values.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testKafkaMetricsSettings() {
        String metricsConfigJson = "{\"lowercaseOutputName\":true}";
        String metricsConfigYaml = "lowercaseOutputName: true";

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(
                JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),
                true
        );

        ConfigMap externalMetricsCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(TestConstants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(namespaceSecond)
                .endMetadata()
                .build();

        kubeClient().createConfigMapInNamespace(namespaceSecond, externalMetricsCm);

        // spec.kafka.metrics -> spec.kafka.jmxExporterMetrics
        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("external-metrics-cm")
                .withKey(TestConstants.METRICS_CONFIG_YAML_NAME)
                .build();
        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceSecond, kafkaClusterSecondName, k -> {
            k.getSpec().getKafka().setMetricsConfig(jmxPrometheusExporterMetrics);
        });

        PodUtils.verifyThatRunningPodsAreStable(namespaceSecond, kafkaClusterSecondName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(namespaceSecond, kafkaClusterSecondName)) {
            ConfigMap actualCm = kubeClient(namespaceSecond).getConfigMap(cmName);
            assertThat(actualCm.getData().get(TestConstants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson));
        }

        // update metrics
        ConfigMap externalMetricsUpdatedCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(TestConstants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml.replace("true", "false")))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(namespaceSecond)
                .endMetadata()
                .build();

        kubeClient().updateConfigMapInNamespace(namespaceSecond, externalMetricsUpdatedCm);
        PodUtils.verifyThatRunningPodsAreStable(namespaceSecond, kafkaClusterSecondName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(namespaceSecond, kafkaClusterSecondName)) {
            ConfigMap actualCm = kubeClient(namespaceSecond).getConfigMap(cmName);
            assertThat(actualCm.getData().get(TestConstants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson.replace("true", "false")));
        }
    }

    @BeforeAll
    void setupEnvironment() {
        // Metrics tests are not designed to run with namespace RBAC scope.
        assumeFalse(Environment.isNamespaceRbacScope());
        NamespaceManager.getInstance().createNamespaces(Environment.TEST_SUITE_NAMESPACE, CollectorElement.createCollectorElement(this.getClass().getName()), Arrays.asList(namespaceFirst, namespaceSecond));

        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        final String coScraperName = TestConstants.CO_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        final String testSuiteScraperName = Environment.TEST_SUITE_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        final String scraperName = namespaceFirst + "-" + TestConstants.SCRAPER_NAME;
        final String secondScraperName = namespaceSecond + "-" + TestConstants.SCRAPER_NAME;

        cluster.setNamespace(namespaceFirst);

        // create resources without wait to deploy them simultaneously
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(namespaceFirst, KafkaNodePoolResource.getBrokerPoolName(kafkaClusterFirstName), kafkaClusterFirstName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(namespaceFirst, KafkaNodePoolResource.getControllerPoolName(kafkaClusterFirstName), kafkaClusterFirstName, 3).build(),
            KafkaNodePoolTemplates.brokerPool(namespaceSecond, KafkaNodePoolResource.getBrokerPoolName(kafkaClusterSecondName), kafkaClusterSecondName, 1).build(),
            KafkaNodePoolTemplates.controllerPool(namespaceSecond, KafkaNodePoolResource.getControllerPoolName(kafkaClusterSecondName), kafkaClusterSecondName, 1).build()
        );
        resourceManager.createResourceWithoutWait(
            KafkaTemplates.kafkaMetricsConfigMap(namespaceFirst, kafkaClusterFirstName),
            KafkaTemplates.cruiseControlMetricsConfigMap(namespaceFirst, kafkaClusterFirstName),
            // Kafka with CruiseControl and metrics
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(namespaceFirst, kafkaClusterFirstName, 3)
                .editOrNewSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalMs(30_000L)
                        .endTopicOperator()
                        .editUserOperator()
                            .withReconciliationIntervalMs(30_000L)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build(),
            KafkaTemplates.kafkaMetricsConfigMap(namespaceSecond, kafkaClusterSecondName),
            KafkaTemplates.kafkaWithMetrics(namespaceSecond, kafkaClusterSecondName, 1).build(),
            ScraperTemplates.scraperPod(TestConstants.CO_NAMESPACE, coScraperName).build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, testSuiteScraperName).build(),
            ScraperTemplates.scraperPod(namespaceFirst, scraperName).build(),
            ScraperTemplates.scraperPod(namespaceSecond, secondScraperName).build()
        );

        // sync resources
        resourceManager.synchronizeResources();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(namespaceFirst, topicName, kafkaClusterFirstName, 7, 2).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(namespaceFirst, kafkaExporterTopicName, kafkaClusterFirstName, 7, 2).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(namespaceFirst, bridgeTopicName, kafkaClusterFirstName).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(namespaceFirst, KafkaUserUtils.generateRandomNameOfKafkaUser(), kafkaClusterFirstName).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(namespaceFirst, KafkaUserUtils.generateRandomNameOfKafkaUser(), kafkaClusterFirstName).build());

        coScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(TestConstants.CO_NAMESPACE, coScraperName).get(0).getMetadata().getName();
        scraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(namespaceFirst, scraperName).get(0).getMetadata().getName();

        // Allow connections from clients to operators pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(TestConstants.CO_NAMESPACE);

        // wait some time for metrics to be stable - at least reconciliation interval + 10s
        LOGGER.info("Sleeping for {} to give operators and operands some time to stable the metrics values before collecting",
                TestConstants.SAFETY_RECONCILIATION_INTERVAL);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(TestConstants.SAFETY_RECONCILIATION_INTERVAL));

        kafkaCollector = new BaseMetricsCollector.Builder()
            .withScraperPodName(scraperPodName)
            .withNamespaceName(namespaceFirst)
            .withComponent(KafkaMetricsComponent.create(kafkaClusterFirstName))
            .build();

        kafkaExporterCollector = kafkaCollector.toBuilder()
            .withComponent(KafkaExporterMetricsComponent.create(namespaceFirst, kafkaClusterFirstName))
            .build();

        clusterOperatorCollector = new BaseMetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, clusterOperator.getClusterOperatorName()))
            .build();

        kafkaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        kafkaExporterCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        clusterOperatorCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
    }
}

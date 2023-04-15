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
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
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
import io.strimzi.test.executor.Exec;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.METRICS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SANITY;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * @description This test suite is designed for testing metrics exposed by operators and operands.
 *
 * @info the class should be executed without issues with all the following combinations
 *  - All install types
 *  - All feature gates
 *  - In parallel with restrictions based on test-case annotations
 *
 * @beforeAll
 *  1. - Create namespaces {@namespaceFirst} and {@namespaceSecond}
 *     - Namespaces {@namespaceFirst} and {@namespaceSecond} are created
 *  2. - Deploy ClusterOperator
 *     - ClusterOperator is deployed
 *  3. - Deploy Kafka {@kafkaClusterFirstName} with metrics and CruiseControl configured
 *     - Kafka @{kafkaClusterFirstName} is deployed
 *  4. - Deploy Kafka {@kafkaClusterSecondtName} with metrics configured
 *     - Kafka @{kafkaClusterFirstName} is deployed
 *  5. - Deploy scraper pods in namespace {@namespaceFirst} and {@namespaceSecond} for collecting metrics from Strimzi pods
 *     - Scraper pods are deployed
 *  6. - Create KafkaUsers and KafkaTopics
 *     - All KafkaUsers and KafkaTopics are Ready
 *  7. - Setup NetworkPolicies to grant access to operator pods and KafkaExporter
 *     - NetworkPolicies created
 *  8. - Create collectors for ClusterOperator, Kafka, KafkaExporter, and Zookeeper (Non-KRaft)
 *     - Metrics collected in collectors structs
 *
 * @afterAll
 *  1. - Common cleaning of all resources created by this test class
 *     - All resources deleted.
 *
 * @updated 2023-17-04
 */
@Tag(SANITY)
@Tag(ACCEPTANCE)
@Tag(REGRESSION)
@Tag(METRICS)
@Tag(CRUISE_CONTROL)
@IsolatedSuite
public class MetricsIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsIsolatedST.class);

    private final String namespaceFirst = "metrics-test-0";
    private final String namespaceSecond = "metrics-test-1";
    private final String kafkaClusterFirstName = "metrics-cluster-0";
    private final String kafkaClusterSecondName = "metrics-cluster-1";
    private final String mm2ClusterName = "mm2-cluster";
    private final String bridgeClusterName = "my-bridge";

    private final Object lock = new Object();
    String coScraperPodName;

    String scraperPodName;
    String secondNamespaceScraperPodName;

    private String bridgeTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private final String kafkaExporterTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

    private MetricsCollector kafkaCollector;
    private MetricsCollector zookeeperCollector;
    private MetricsCollector kafkaExporterCollector;
    private MetricsCollector clusterOperatorCollector;

    /**
     * Description: This test suite is designed for testing metrics exposed by operators and operands.
     *
     * Steps:
     *
     * UseCase:
     */
    @ParallelTest
    @Tag(ACCEPTANCE)
    void testKafkaMetrics() {
        assertMetricValueCount(kafkaCollector, "kafka_server_replicamanager_leadercount", 3);
        assertMetricCountHigherThan(kafkaCollector, "kafka_server_replicamanager_partitioncount", 2);
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_underreplicatedpartitions", 0);
        assertMetricValue(kafkaCollector, "kafka_controller_kafkacontroller_activecontrollercount", 1);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperMetrics() {
        assertMetricValueNotNull(zookeeperCollector, "zookeeper_quorumsize");
        assertMetricCountHigherThan(zookeeperCollector, "zookeeper_numaliveconnections\\{.*\\}", 0L);
        assertMetricCountHigherThan(zookeeperCollector, "zookeeper_inmemorydatatree_watchcount\\{.*\\}", 0L);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectMetrics(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext,
            KafkaConnectTemplates.kafkaConnectWithMetricsAndFileSinkPlugin(kafkaClusterFirstName, namespaceFirst, kafkaClusterFirstName, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build());
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(kafkaClusterFirstName).build());

        MetricsCollector kafkaConnectCollector = kafkaCollector.toBuilder()
                .withComponentType(ComponentType.KafkaConnect)
                .build();

        kafkaConnectCollector.collectMetricsFromPods();

        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_request_total\\{clientid=\".*\",}", 0);
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_response_total\\{clientid=\".*\",.*}", 0);
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_network_io_total\\{clientid=\".*\",.*}", 0);

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods();
        assertCoMetricResources(KafkaConnect.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(KafkaConnect.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceState(KafkaConnect.RESOURCE_KIND, kafkaClusterFirstName, namespaceFirst, 1, "none");

        assertCoMetricResources(KafkaConnector.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(KafkaConnector.RESOURCE_KIND, namespaceSecond);
    }


    @IsolatedTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaExporterDataAfterExchange(ExtensionContext extensionContext) {
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String kafkaStrimziPodSetName = KafkaResources.kafkaStatefulSetName(kafkaClusterFirstName);
        final LabelSelector kafkaPodsSelector = KafkaResource.getLabelSelector(kafkaClusterFirstName, kafkaStrimziPodSetName);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(kafkaExporterTopicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterFirstName))
            .withNamespaceName(namespaceFirst)
            .withMessageCount(5000)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(producerName, consumerName, namespaceFirst, MESSAGE_COUNT, false);

        assertMetricValueNotNull(kafkaExporterCollector, "kafka_consumergroup_current_offset\\{.*\\}");

        if (!Environment.isKRaftModeEnabled()) {
            Pattern pattern = Pattern.compile("kafka_topic_partitions\\{topic=\"" + kafkaExporterTopicName + "\"} ([\\d])");
            ArrayList<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s doesn't contain correct value", pattern), values.stream().mapToDouble(i -> i).sum(), is(7.0));
        }

        kubeClient().listPods(namespaceFirst, kafkaPodsSelector).forEach(pod -> {
            String address = pod.getMetadata().getName() + "." + kafkaClusterFirstName + "-kafka-brokers." + namespaceFirst + ".svc";
            Pattern pattern = Pattern.compile("kafka_broker_info\\{address=\"" + address + ".*\",.*} ([\\d])");
            ArrayList<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s is not null", pattern), values, notNullValue());
        });
    }

    @ParallelTest
    void testKafkaExporterDifferentSetting() throws InterruptedException, ExecutionException, IOException {
        LabelSelector exporterSelector = kubeClient().getDeploymentSelectors(namespaceFirst, KafkaExporterResources.deploymentName(kafkaClusterFirstName));
        String runScriptContent = getExporterRunScript(kubeClient().listPods(namespaceFirst, exporterSelector).get(0).getMetadata().getName(), namespaceFirst);
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\".*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\".*\""));

        Map<String, String> kafkaExporterSnapshot = DeploymentUtils.depSnapshot(namespaceFirst, KafkaExporterResources.deploymentName(kafkaClusterFirstName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(kafkaClusterFirstName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex("my-group.*");
            k.getSpec().getKafkaExporter().setTopicRegex(topicName);
        }, namespaceFirst);

        kafkaExporterSnapshot = DeploymentUtils.waitTillDepHasRolled(namespaceFirst, KafkaExporterResources.deploymentName(kafkaClusterFirstName), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(kubeClient().listPods(namespaceFirst, exporterSelector).get(0).getMetadata().getName(), namespaceFirst);
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + topicName + "\""));

        LOGGER.info("Changing topic and group regexes back to default");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(kafkaClusterFirstName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex(".*");
            k.getSpec().getKafkaExporter().setTopicRegex(".*");
        }, namespaceFirst);

        DeploymentUtils.waitTillDepHasRolled(namespaceFirst, KafkaExporterResources.deploymentName(kafkaClusterFirstName), 1, kafkaExporterSnapshot);
    }

    @ParallelTest
    void testClusterOperatorMetrics() {
        assertCoMetricResourceNotNull("strimzi_reconciliations_periodical_total", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_count", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_sum", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_max", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_successful_total", Kafka.RESOURCE_KIND);

        assertCoMetricResources(Kafka.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResources(Kafka.RESOURCE_KIND, namespaceSecond, 1);
        assertCoMetricResourceState(Kafka.RESOURCE_KIND, kafkaClusterFirstName, namespaceFirst, 1, "none");
        assertCoMetricResourceState(Kafka.RESOURCE_KIND, kafkaClusterSecondName, namespaceSecond, 1, "none");

        assertCoMetricResourcesNullOrZero(KafkaMirrorMaker.RESOURCE_KIND, namespaceFirst);
        assertCoMetricResourcesNullOrZero(KafkaMirrorMaker.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceStateNotExists(KafkaMirrorMaker.RESOURCE_KIND, namespaceFirst, kafkaClusterFirstName);

        assertCoMetricResourcesNullOrZero(KafkaRebalance.RESOURCE_KIND, namespaceFirst);
        assertCoMetricResourcesNullOrZero(KafkaRebalance.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceStateNotExists(KafkaRebalance.RESOURCE_KIND, namespaceFirst, kafkaClusterFirstName);
    }

    @ParallelTest
    void testUserOperatorMetrics() {
        MetricsCollector userOperatorCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.UserOperator)
            .build();

        userOperatorCollector.collectMetricsFromPods();

        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_successful_total", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_count", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_sum", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_max", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_periodical_total", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_total", KafkaUser.RESOURCE_KIND);

        assertMetricResources(userOperatorCollector, KafkaUser.RESOURCE_KIND, namespaceFirst, 2);
    }

    @ParallelTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test case")
    void testTopicOperatorMetrics() {
        MetricsCollector topicOperatorCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.TopicOperator)
            .build();

        topicOperatorCollector.collectMetricsFromPods();

        assertMetricResourceNotNull(topicOperatorCollector, "strimzi_reconciliations_successful_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(topicOperatorCollector, "strimzi_reconciliations_duration_seconds_count", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(topicOperatorCollector, "strimzi_reconciliations_duration_seconds_sum", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(topicOperatorCollector, "strimzi_reconciliations_duration_seconds_max", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(topicOperatorCollector, "strimzi_reconciliations_periodical_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(topicOperatorCollector, "strimzi_reconciliations_total", KafkaTopic.RESOURCE_KIND);

        cmdKubeClient().list(KafkaTopic.RESOURCE_KIND).forEach(topicName -> {
            LOGGER.info("{}: {}", KafkaTopic.RESOURCE_KIND, topicName);
        });

        assertMetricResourcesHigherThanOrEqualTo(topicOperatorCollector, KafkaTopic.RESOURCE_KIND, getExpectedTopics().size());
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testMirrorMaker2Metrics(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext,
                KafkaMirrorMaker2Templates.kafkaMirrorMaker2WithMetrics(namespaceFirst, mm2ClusterName, kafkaClusterFirstName, kafkaClusterSecondName, 1, namespaceSecond, namespaceFirst)
                    .editMetadata()
                        .withNamespace(namespaceFirst)
                    .endMetadata()
                    .build());

        MetricsCollector kmm2Collector = kafkaCollector.toBuilder()
            .withComponentName(mm2ClusterName)
            .withComponentType(ComponentType.KafkaMirrorMaker2)
            .build();

        assertMetricValue(kmm2Collector, "kafka_connect_worker_connector_count", 3);
        assertMetricValue(kmm2Collector, "kafka_connect_worker_task_count", 1);

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods();
        assertCoMetricResources(KafkaMirrorMaker2.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(KafkaMirrorMaker2.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceState(KafkaMirrorMaker2.RESOURCE_KIND, mm2ClusterName, namespaceFirst, 1, "none");
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testKafkaBridgeMetrics(ExtensionContext extensionContext) {
        String producerName = "bridge-producer";
        String consumerName = "bridge-consumer";

        resourceManager.createResource(extensionContext,
                KafkaBridgeTemplates.kafkaBridgeWithMetrics(bridgeClusterName, kafkaClusterFirstName, KafkaResources.plainBootstrapAddress(kafkaClusterFirstName), 1)
                    .editMetadata()
                        .withNamespace(namespaceFirst)
                    .endMetadata()
                    .build());

        MetricsCollector bridgeCollector = kafkaCollector.toBuilder()
            .withComponentName(bridgeClusterName)
            .withComponentType(ComponentType.KafkaBridge)
            .build();

        // Attach consumer before producer
        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withNamespaceName(namespaceFirst)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(bridgeClusterName))
            .withTopicName(bridgeTopicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(200)
            .withPollInterval(200)
            .build();

        // we cannot wait for producer and consumer to complete to see all needed metrics - especially `strimzi_bridge_kafka_producer_count`
        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge(), kafkaBridgeClientJob.consumerStrimziBridge());

        bridgeCollector.collectMetricsFromPods();
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_producer_count\\{.*,}");
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_consumer_connection_count\\{.*,}");
        assertThat("bridge collected data don't contain strimzi_bridge_http_server", bridgeCollector.getCollectedData().values().toString().contains("strimzi_bridge_http_server"));

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods();
        assertCoMetricResources(KafkaBridge.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(KafkaBridge.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceState(KafkaBridge.RESOURCE_KIND, bridgeClusterName, namespaceFirst, 1, "none");
    }

    @ParallelTest
    void testCruiseControlMetrics() {
        String cruiseControlMetrics = CruiseControlUtils.callApi(namespaceFirst, CruiseControlUtils.SupportedHttpMethods.GET, "/metrics");

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

    /**
     * 1. Update metrics from whatever it is to @metricsConfigYaml in spec.kafka.metricsConfig
     * 2. Check, whether the metrics ConfigMap is changed
     * 3. Updates ConfigMap linked as metrics on
     * 4. Check, whether the metrics ConfigMap is changed
     */
    @ParallelTest
    void testKafkaMetricsSettings() {
        String metricsConfigJson = "{\"lowercaseOutputName\":true}";
        String metricsConfigYaml = "lowercaseOutputName: true";

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(
                JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),
                true
        );

        ConfigMap externalMetricsCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(Constants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(namespaceSecond)
                .endMetadata()
                .build();

        kubeClient().createConfigMapInNamespace(namespaceSecond, externalMetricsCm);

        // spec.kafka.metrics -> spec.kafka.jmxExporterMetrics
        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("external-metrics-cm")
                .withKey(Constants.METRICS_CONFIG_YAML_NAME)
                .build();
        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();
        KafkaResource.replaceKafkaResourceInSpecificNamespace(kafkaClusterSecondName, k -> {
            k.getSpec().getKafka().setMetricsConfig(jmxPrometheusExporterMetrics);
        }, namespaceSecond);

        PodUtils.verifyThatRunningPodsAreStable(namespaceSecond, kafkaClusterSecondName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(kafkaClusterSecondName, 1)) {
            ConfigMap actualCm = kubeClient(namespaceSecond).getConfigMap(cmName);
            assertThat(actualCm.getData().get(Constants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson));
        }

        // update metrics
        ConfigMap externalMetricsUpdatedCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(Constants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml.replace("true", "false")))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(namespaceSecond)
                .endMetadata()
                .build();

        kubeClient().updateConfigMapInNamespace(namespaceSecond, externalMetricsUpdatedCm);
        PodUtils.verifyThatRunningPodsAreStable(namespaceSecond, kafkaClusterSecondName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(kafkaClusterSecondName, 1)) {
            ConfigMap actualCm = kubeClient(namespaceSecond).getConfigMap(cmName);
            assertThat(actualCm.getData().get(Constants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson.replace("true", "false")));
        }
    }

    @ParallelTest
    void testStrimziPodSetMetrics() {
        // Expected PodSet counts per component
        int zooPodSetCount = Environment.isKRaftModeEnabled() ? 0 : 1;
        int kafkaPodSetCount = 1;
        int connectAndMm2PodSetCount = Environment.isStableConnectIdentitiesEnabled() ? 2 : 0;

        // check StrimziPodSet metrics in CO
        assertCoMetricResources(StrimziPodSet.RESOURCE_KIND, namespaceFirst, zooPodSetCount + kafkaPodSetCount + connectAndMm2PodSetCount);
        assertCoMetricResources(StrimziPodSet.RESOURCE_KIND, namespaceSecond, zooPodSetCount + kafkaPodSetCount);

        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_bucket", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_count", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_sum", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_max", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_already_enqueued_total", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_successful_total", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull("strimzi_reconciliations_total", StrimziPodSet.RESOURCE_KIND);
    }

    @IsolatedTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testReconcileStateMetricInTopicOperator(ExtensionContext extensionContext) {
        cluster.setNamespace(namespaceSecond);

        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int initialReplicas = 1;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSecondName, topicName, 1, initialReplicas).build());

        String secondClientsPodName = kubeClient(namespaceSecond).listPodsByPrefixInName(namespaceSecond + "-" + Constants.SCRAPER_NAME).get(0).getMetadata().getName();

        MetricsCollector secondNamespaceCollector = new MetricsCollector.Builder()
            .withNamespaceName(namespaceSecond)
            .withScraperPodName(secondClientsPodName)
            .withComponentName(kafkaClusterSecondName)
            .withComponentType(ComponentType.TopicOperator)
            .build();

        String reasonMessage = "none";

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, namespaceSecond, 1, reasonMessage);

        LOGGER.info("Changing topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"), cluster.getNamespace());
        KafkaTopicUtils.waitForKafkaTopicNotReady(cluster.getNamespace(), topicName);

        reasonMessage = "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.";
        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, namespaceSecond, 0, reasonMessage);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(topicName);
            kafkaTopic.getSpec().setReplicas(12);
        }, cluster.getNamespace());

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(namespaceSecond, topicName, 12);

        reasonMessage = "Changing 'spec.replicas' is not supported. .*";
        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, namespaceSecond, 0, reasonMessage);

        LOGGER.info("Scaling replicas to be higher than before");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(24), cluster.getNamespace());

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(namespaceSecond, topicName, 24);

        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, namespaceSecond, 0, reasonMessage);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas), cluster.getNamespace());
        KafkaTopicUtils.waitForKafkaTopicReady(cluster.getNamespace(), topicName);

        reasonMessage = "none";

        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, namespaceSecond, 1, reasonMessage);

        cluster.setNamespace(clusterOperator.getDeploymentNamespace());
    }

    private String getExporterRunScript(String podName, String namespace) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("cat");
        command.add("/tmp/run.sh");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", namespace, "--"));
        executableCommand.addAll(command);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the pod
        int ret = exec.execute(null, executableCommand, 20_000);

        synchronized (lock) {
            LOGGER.info("Metrics collection for pod {} return code - {}", podName, ret);
        }

        assertThat("Collected metrics should not be empty", exec.out(), not(emptyString()));
        return exec.out();
    }

    private void assertCoMetricResourceNotNull(String metric, String kind) {
        assertMetricResourceNotNull(clusterOperatorCollector, metric, kind);
    }

    private void assertMetricResourceNotNull(MetricsCollector collector, String metric, String kind) {
        String metrics = metric + "\\{kind=\"" + kind + "\",.*}";
        assertMetricValueNotNull(collector, metrics);
    }

    private void assertCoMetricResourceStateNotExists(String kind, String name, String namespace) {
        String metric = "strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",resource_namespace=\"" + namespace + "\",}";
        ArrayList<Double> values = createPatternAndCollectWithoutWait(clusterOperatorCollector, metric);
        assertThat(values.isEmpty(), is(true));
    }

    private void assertCoMetricResourceState(String kind, String name, String namespace, int value, String reason) {
        assertMetricResourceState(clusterOperatorCollector, kind, name, namespace, value, reason);
    }

    private void assertMetricResourceState(MetricsCollector collector, String kind, String name, String namespace, int value, String reason) {
        String metric = "strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",reason=\"" + reason + "\",resource_namespace=\"" + namespace + "\",}";
        assertMetricValue(collector, metric, value);
    }

    private void assertCoMetricResources(String kind, String namespace, int value) {
        assertMetricResources(clusterOperatorCollector, kind, namespace, value);
    }

    private void assertMetricResources(MetricsCollector collector, String kind, String namespace, int value) {
        assertMetricValue(collector, getResourceMetricPattern(kind, namespace), value);
    }

    private void assertCoMetricResourcesNullOrZero(String kind, String namespace) {
        Pattern pattern = Pattern.compile(getResourceMetricPattern(kind, namespace));
        if (!clusterOperatorCollector.collectSpecificMetric(pattern).isEmpty()) {
            assertThat(String.format("metric %s doesn't contain 0 value!", pattern), createPatternAndCollectWithoutWait(clusterOperatorCollector, pattern.toString()).stream().mapToDouble(i -> i).sum(), is(0.0));
        }
    }

    private String getResourceMetricPattern(String kind, String namespace) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",";
        metric += namespace == null ? ".*}" : "namespace=\"" + namespace + "\",.*}";
        return metric;
    }

    private void assertMetricResourcesHigherThanOrEqualTo(MetricsCollector collector, String kind, int expectedValue) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",.*}";
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' doesn't have expected value: %s < %s", metric, actualValue, expectedValue), actualValue >= expectedValue);
    }
    private void assertMetricValueNotNull(MetricsCollector collector, String metric) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' doesn't exist", metric), actualValue, notNullValue());
    }

    private void assertMetricValue(MetricsCollector collector, String metric, int expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue, is((double) expectedValue));
    }

    private void assertMetricValueCount(MetricsCollector collector, String metric, long expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", actualValue, expectedValue, metric), actualValue, is((double) expectedValue));
    }

    private void assertMetricCountHigherThan(MetricsCollector collector, String metric, long expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s not is higher than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    private void assertMetricValueHigherThan(MetricsCollector collector, String metric, int expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    private ArrayList<Double> createPatternAndCollect(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.waitForSpecificMetricAndCollect(pattern);
    }

    private ArrayList<Double> createPatternAndCollectWithoutWait(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.collectSpecificMetric(pattern);
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) throws Exception {
        clusterOperator.unInstall();
        cluster.createNamespaces(CollectorElement.createCollectorElement(this.getClass().getName()), clusterOperator.getDeploymentNamespace(), Arrays.asList(namespaceFirst, namespaceSecond));

        clusterOperator = clusterOperator.defaultInstallation()
//            .withWatchingNamespaces(clusterOperator.getDeploymentNamespace() + "," + namespaceFirst + "," + namespaceSecond)
//            .withBindingsNamespaces(Arrays.asList(clusterOperator.getDeploymentNamespace(), namespaceFirst, namespaceSecond))
            .createInstallation()
            .runInstallation();

        final String coScraperName = clusterOperator.getDeploymentNamespace() + "-" + Constants.SCRAPER_NAME;
        final String scraperName = namespaceFirst + "-" + Constants.SCRAPER_NAME;
        final String secondScraperName = namespaceSecond + "-" + Constants.SCRAPER_NAME;

        cluster.setNamespace(namespaceFirst);

        // create resources without wait to deploy them simultaneously
        resourceManager.createResource(extensionContext, false,
            // kafka with cruise control and metrics
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(kafkaClusterFirstName, namespaceFirst, 3, 3)
                .editOrNewSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(30)
                        .endTopicOperator()
                        .editUserOperator()
                            .withReconciliationIntervalSeconds(30)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build(),
            KafkaTemplates.kafkaWithMetrics(kafkaClusterSecondName, namespaceSecond, 1, 1).build(),
            ScraperTemplates.scraperPod(clusterOperator.getDeploymentNamespace(), coScraperName).build(),
            ScraperTemplates.scraperPod(namespaceFirst, scraperName).build(),
            ScraperTemplates.scraperPod(namespaceSecond, secondScraperName).build()
        );

        // sync resources
        resourceManager.synchronizeResources(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterFirstName, topicName, 7, 2).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterFirstName, kafkaExporterTopicName, 7, 2).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterFirstName, bridgeTopicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceFirst, kafkaClusterFirstName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceFirst, kafkaClusterFirstName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());

        coScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), coScraperName).get(0).getMetadata().getName();
        scraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(namespaceFirst, scraperName).get(0).getMetadata().getName();
        secondNamespaceScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(namespaceSecond, secondScraperName).get(0).getMetadata().getName();

        // Allow connections from clients to operators pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(extensionContext, clusterOperator.getDeploymentNamespace());
        NetworkPolicyResource.allowNetworkPolicySettingsForEntityOperator(extensionContext, kafkaClusterFirstName, namespaceFirst);
        NetworkPolicyResource.allowNetworkPolicySettingsForEntityOperator(extensionContext, kafkaClusterSecondName, namespaceSecond);
        // Allow connections from clients to KafkaExporter when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, kafkaClusterFirstName, namespaceFirst);
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, kafkaClusterSecondName, namespaceSecond);

        // wait some time for metrics to be stable - at least reconciliation interval + 10s
        LOGGER.info("Sleeping for {} to give operators and operands some time to stable the metrics values before collecting",
                Constants.SAFETY_RECONCILIATION_INTERVAL);
        Thread.sleep(Constants.SAFETY_RECONCILIATION_INTERVAL);

        kafkaCollector = new MetricsCollector.Builder()
            .withScraperPodName(scraperPodName)
            .withNamespaceName(namespaceFirst)
            .withComponentType(ComponentType.Kafka)
            .withComponentName(kafkaClusterFirstName)
            .build();

        if (!Environment.isKRaftModeEnabled()) {
            zookeeperCollector = kafkaCollector.toBuilder()
                    .withComponentType(ComponentType.Zookeeper)
                    .build();
            zookeeperCollector.collectMetricsFromPods();
        }

        kafkaExporterCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.KafkaExporter)
            .build();

        clusterOperatorCollector = new MetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withComponentType(ComponentType.ClusterOperator)
            .withComponentName("")
            .build();

        kafkaCollector.collectMetricsFromPods();
        kafkaExporterCollector.collectMetricsFromPods();
        clusterOperatorCollector.collectMetricsFromPods();
    }

    private List<String> getExpectedTopics() {
        ArrayList<String> list = new ArrayList<>();
        list.add("heartbeats");
        list.add(topicName);
        list.add(bridgeTopicName);
        list.add(kafkaExporterTopicName);
        list.add(CruiseControlUtils.CRUISE_CONTROL_METRICS_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        list.add("strimzi-store-topic");
        list.add("strimzi-topic-operator-kstreams-topic-store-changelog");
        list.add("__consumer-offsets---hash");
        return list;
    }
}

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
import io.strimzi.systemtest.annotations.StrimziPodSetTest;
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

@Tag(SANITY)
@Tag(ACCEPTANCE)
@Tag(REGRESSION)
@Tag(METRICS)
@Tag(CRUISE_CONTROL)
@IsolatedSuite
public class MetricsIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsIsolatedST.class);

    public static final String SECOND_NAMESPACE = Constants.METRICS_SECOND_NAMESPACE;
    public static final String SECOND_CLUSTER = "second-kafka-cluster";
    public static final String MIRROR_MAKER_CLUSTER = "mm2-cluster";
    private static final String BRIDGE_CLUSTER = "my-bridge";
    private final String metricsClusterName = "metrics-cluster-name";
    private final Object lock = new Object();
    String scraperPodName;
    String secondNamespaceScraperPodName;

    private String bridgeTopic = KafkaTopicUtils.generateRandomNameOfTopic();
    private String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private final String kafkaExporterTopic = KafkaTopicUtils.generateRandomNameOfTopic();

    private MetricsCollector kafkaCollector;
    private MetricsCollector zookeeperCollector;
    private MetricsCollector kafkaConnectCollector;
    private MetricsCollector kafkaExporterCollector;
    private MetricsCollector clusterOperatorCollector;

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testKafkaBrokersCount() {
        assertMetricValueCount(kafkaCollector, "kafka_server_replicamanager_leadercount", 3);
    }

    @ParallelTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test case")
    void testKafkaTopicPartitions() {
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_partitioncount", 507);
    }

    @ParallelTest
    void testKafkaTopicUnderReplicatedPartitions() {
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_underreplicatedpartitions", 0);
    }

    @ParallelTest
    void testKafkaActiveControllers() {
        assertMetricValue(kafkaCollector, "kafka_controller_kafkacontroller_activecontrollercount", 1);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperQuorumSize() {
        assertMetricValueNotNull(zookeeperCollector, "zookeeper_quorumsize");
    }

    @ParallelTest
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperAliveConnections() {
        assertMetricCountHigherThan(zookeeperCollector, "zookeeper_numaliveconnections\\{.*\\}", 0L);
    }

    @ParallelTest
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperWatchersCount() {
        assertMetricCountHigherThan(zookeeperCollector, "zookeeper_inmemorydatatree_watchcount\\{.*\\}", 0L);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectRequests() {
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_request_total\\{clientid=\".*\",}", 0);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectResponse() {
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_response_total\\{clientid=\".*\",.*}", 0);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectIoNetwork() {
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_network_io_total\\{clientid=\".*\",.*}", 0);
    }

    @IsolatedTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaExporterDataAfterExchange(ExtensionContext extensionContext) {
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String kafkaName = KafkaResources.kafkaStatefulSetName(metricsClusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(metricsClusterName, kafkaName);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(kafkaExporterTopic)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(metricsClusterName))
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withMessageCount(5000)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(producerName, consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT, false);

        assertMetricValueNotNull(kafkaExporterCollector, "kafka_consumergroup_current_offset\\{.*\\}");

        if (!Environment.isKRaftModeEnabled()) {
            Pattern pattern = Pattern.compile("kafka_topic_partitions\\{topic=\"" + kafkaExporterTopic + "\"} ([\\d])");
            ArrayList<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s doesn't contain correct value", pattern), values.stream().mapToDouble(i -> i).sum(), is(7.0));
        }

        kubeClient().listPods(clusterOperator.getDeploymentNamespace(), kafkaSelector).forEach(pod -> {
            String address = pod.getMetadata().getName() + "." + metricsClusterName + "-kafka-brokers." + clusterOperator.getDeploymentNamespace() + ".svc";
            Pattern pattern = Pattern.compile("kafka_broker_info\\{address=\"" + address + ".*\",.*} ([\\d])");
            ArrayList<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s is not null", pattern), values, notNullValue());
        });
    }

    @ParallelTest
    void testKafkaExporterDifferentSetting() throws InterruptedException, ExecutionException, IOException {
        LabelSelector exporterSelector = kubeClient().getDeploymentSelectors(clusterOperator.getDeploymentNamespace(), KafkaExporterResources.deploymentName(metricsClusterName));
        String runScriptContent = getExporterRunScript(kubeClient().listPods(clusterOperator.getDeploymentNamespace(), exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\".*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\".*\""));

        Map<String, String> kafkaExporterSnapshot = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), KafkaExporterResources.deploymentName(metricsClusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(metricsClusterName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex("my-group.*");
            k.getSpec().getKafkaExporter().setTopicRegex(topicName);
        }, clusterOperator.getDeploymentNamespace());

        kafkaExporterSnapshot = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), KafkaExporterResources.deploymentName(metricsClusterName), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(kubeClient().listPods(clusterOperator.getDeploymentNamespace(), exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + topicName + "\""));

        LOGGER.info("Changing topic and group regexes back to default");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(metricsClusterName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex(".*");
            k.getSpec().getKafkaExporter().setTopicRegex(".*");
        }, clusterOperator.getDeploymentNamespace());

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), KafkaExporterResources.deploymentName(metricsClusterName), 1, kafkaExporterSnapshot);
    }

    @ParallelTest
    void testClusterOperatorMetrics() {
        List<String> resourcesList = Arrays.asList(
            Kafka.RESOURCE_KIND, KafkaBridge.RESOURCE_KIND, KafkaConnect.RESOURCE_KIND, KafkaConnector.RESOURCE_KIND,
            KafkaMirrorMaker.RESOURCE_KIND, KafkaMirrorMaker2.RESOURCE_KIND, KafkaRebalance.RESOURCE_KIND
        );

        for (String resource : resourcesList) {
            if (!resource.equals(KafkaConnector.RESOURCE_KIND)) {
                assertCoMetricResourceNotNull("strimzi_reconciliations_periodical_total", resource);
            }

            if (!resource.equals(KafkaMirrorMaker.RESOURCE_KIND) && !resource.equals(KafkaRebalance.RESOURCE_KIND)) {
                assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_count", resource);
                assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_sum", resource);
                assertCoMetricResourceNotNull("strimzi_reconciliations_duration_seconds_max", resource);
                assertCoMetricResourceNotNull("strimzi_reconciliations_successful_total", resource);
            }
        }

        assertCoMetricResources(Kafka.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 1);
        assertCoMetricResources(Kafka.RESOURCE_KIND, SECOND_NAMESPACE, 1);
        assertCoMetricResourceState(Kafka.RESOURCE_KIND, metricsClusterName, clusterOperator.getDeploymentNamespace(), 1, "none");
        assertCoMetricResourceState(Kafka.RESOURCE_KIND, SECOND_CLUSTER, SECOND_NAMESPACE, 1, "none");

        assertCoMetricResources(KafkaBridge.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 1);
        assertCoMetricResources(KafkaBridge.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceState(KafkaBridge.RESOURCE_KIND, BRIDGE_CLUSTER, clusterOperator.getDeploymentNamespace(), 1, "none");

        assertCoMetricResources(KafkaConnect.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 1);
        assertCoMetricResources(KafkaConnect.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceState(KafkaConnect.RESOURCE_KIND, metricsClusterName, clusterOperator.getDeploymentNamespace(), 1, "none");

        assertCoMetricResources(KafkaMirrorMaker.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 0);
        assertCoMetricResources(KafkaMirrorMaker.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceStateNotExists(KafkaMirrorMaker.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), metricsClusterName);

        assertCoMetricResources(KafkaMirrorMaker2.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 1);
        assertCoMetricResources(KafkaMirrorMaker2.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceState(KafkaMirrorMaker2.RESOURCE_KIND, MIRROR_MAKER_CLUSTER, clusterOperator.getDeploymentNamespace(), 1, "none");

        assertCoMetricResources(KafkaConnector.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 1);
        assertCoMetricResources(KafkaConnector.RESOURCE_KIND, SECOND_NAMESPACE, 0);

        assertCoMetricResources(KafkaRebalance.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 0);
        assertCoMetricResources(KafkaRebalance.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceStateNotExists(KafkaRebalance.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), metricsClusterName);
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

        assertMetricResources(userOperatorCollector, KafkaUser.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), 2);
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
    void testMirrorMaker2Metrics() {
        MetricsCollector kmm2Collector = kafkaCollector.toBuilder()
            .withComponentName(MIRROR_MAKER_CLUSTER)
            .withComponentType(ComponentType.KafkaMirrorMaker2)
            .build();

        assertMetricValue(kmm2Collector, "kafka_connect_worker_connector_count", 3);
        assertMetricValue(kmm2Collector, "kafka_connect_worker_task_count", 1);
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testKafkaBridgeMetrics(ExtensionContext extensionContext) {
        String producerName = "bridge-producer";
        String consumerName = "bridge-consumer";

        MetricsCollector bridgeCollector = kafkaCollector.toBuilder()
            .withComponentName(BRIDGE_CLUSTER)
            .withComponentType(ComponentType.KafkaBridge)
            .build();

        // Attach consumer before producer
        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(BRIDGE_CLUSTER))
            .withTopicName(bridgeTopic)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(200)
            .withPollInterval(200)
            .build();

        // we cannot wait for producer and consumer to complete to see all needed metrics - especially `strimzi_bridge_kafka_producer_count`
        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge(), kafkaBridgeClientJob.consumerStrimziBridge());

        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_producer_count\\{.*,}");
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_consumer_connection_count\\{.*,}");
        assertThat("bridge collected data don't contain strimzi_bridge_http_server", bridgeCollector.getCollectedData().values().toString().contains("strimzi_bridge_http_server"));
    }

    @ParallelTest
    void testCruiseControlMetrics() {
        String cruiseControlMetrics = CruiseControlUtils.callApi(clusterOperator.getDeploymentNamespace(), CruiseControlUtils.SupportedHttpMethods.GET, "/metrics");

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
     * 1. Update metrics form whatever it is to @metricsConfigYaml in spec.kafka.metricsConfig
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
                    .withNamespace(SECOND_NAMESPACE)
                .endMetadata()
                .build();

        kubeClient().getClient().configMaps().inNamespace(SECOND_NAMESPACE).resource(externalMetricsCm).createOrReplace();

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
        KafkaResource.replaceKafkaResourceInSpecificNamespace(SECOND_CLUSTER, k -> {
            k.getSpec().getKafka().setMetricsConfig(jmxPrometheusExporterMetrics);
        }, SECOND_NAMESPACE);

        PodUtils.verifyThatRunningPodsAreStable(SECOND_NAMESPACE, SECOND_CLUSTER);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(SECOND_CLUSTER, 1)) {
            ConfigMap actualCm = kubeClient(SECOND_NAMESPACE).getConfigMap(cmName);
            assertThat(actualCm.getData().get(Constants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson));
        }

        // update metrics
        ConfigMap externalMetricsUpdatedCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(Constants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml.replace("true", "false")))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(SECOND_NAMESPACE)
                .endMetadata()
                .build();

        kubeClient().getClient().configMaps().inNamespace(SECOND_NAMESPACE).resource(externalMetricsUpdatedCm).createOrReplace();
        PodUtils.verifyThatRunningPodsAreStable(SECOND_NAMESPACE, SECOND_CLUSTER);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(SECOND_CLUSTER, 1)) {
            ConfigMap actualCm = kubeClient(SECOND_NAMESPACE).getConfigMap(cmName);
            assertThat(actualCm.getData().get(Constants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson.replace("true", "false")));
        }
    }

    @ParallelTest
    @StrimziPodSetTest
    void testStrimziPodSetMetrics() {
        // check StrimziPodSet metrics in CO
        assertCoMetricResources(StrimziPodSet.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), Environment.isKRaftModeEnabled() ? 1 : 2);
        assertCoMetricResources(StrimziPodSet.RESOURCE_KIND, SECOND_NAMESPACE, Environment.isKRaftModeEnabled() ? 1 : 2);

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
        cluster.setNamespace(SECOND_NAMESPACE);

        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int initialReplicas = 1;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(SECOND_CLUSTER, topicName, 1, initialReplicas).build());

        String secondClientsPodName = kubeClient(SECOND_NAMESPACE).listPodsByPrefixInName(SECOND_NAMESPACE + "-" + Constants.SCRAPER_NAME).get(0).getMetadata().getName();

        MetricsCollector secondNamespaceCollector = new MetricsCollector.Builder()
            .withNamespaceName(SECOND_NAMESPACE)
            .withScraperPodName(secondClientsPodName)
            .withComponentName(SECOND_CLUSTER)
            .withComponentType(ComponentType.TopicOperator)
            .build();

        String reasonMessage = "none";

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, SECOND_NAMESPACE, 1, reasonMessage);

        LOGGER.info("Changing topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"), cluster.getNamespace());
        KafkaTopicUtils.waitForKafkaTopicNotReady(cluster.getNamespace(), topicName);

        reasonMessage = "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.";
        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, SECOND_NAMESPACE, 0, reasonMessage);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(topicName);
            kafkaTopic.getSpec().setReplicas(12);
        }, cluster.getNamespace());

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(SECOND_NAMESPACE, topicName, 12);

        reasonMessage = "Changing 'spec.replicas' is not supported. .*";
        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, SECOND_NAMESPACE, 0, reasonMessage);

        LOGGER.info("Scaling replicas to be higher than before");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(24), cluster.getNamespace());

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(SECOND_NAMESPACE, topicName, 24);

        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, SECOND_NAMESPACE, 0, reasonMessage);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas), cluster.getNamespace());
        KafkaTopicUtils.waitForKafkaTopicReady(cluster.getNamespace(), topicName);

        reasonMessage = "none";

        assertMetricResourceState(secondNamespaceCollector, KafkaTopic.RESOURCE_KIND, topicName, SECOND_NAMESPACE, 1, reasonMessage);

        cluster.setNamespace(clusterOperator.getDeploymentNamespace());
    }

    private String getExporterRunScript(String podName) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("cat");
        command.add("/tmp/run.sh");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", clusterOperator.getDeploymentNamespace(), "--"));
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
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",";
        metric += namespace == null ? ".*}" : "namespace=\"" + namespace + "\",.*}";
        assertMetricValue(collector, metric, value);
    }

    private void assertMetricResourcesHigherThanOrEqualTo(MetricsCollector collector, String kind, int value) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",.*}";
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        assertThat(String.format("metric's (%s) values is not higher than %s", metric, value), values.stream().mapToDouble(i -> i).sum() >= value);
    }
    private void assertMetricValueNotNull(MetricsCollector collector, String metric) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        assertThat(String.format("metric %s doesn't exist", metric), values.stream().mapToDouble(i -> i).count(), notNullValue());
    }

    private void assertMetricValue(MetricsCollector collector, String metric, int value) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        assertThat(String.format("metric %s is not containing value %s", metric, value), values.stream().mapToDouble(i -> i).sum(), is((double) value));
    }

    private void assertMetricValueCount(MetricsCollector collector, String metric, long count) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        assertThat(String.format("count %s of metric %s is not correct", count, metric), values.stream().mapToDouble(i -> i).count(), is(count));
    }

    private void assertMetricCountHigherThan(MetricsCollector collector, String metric, long count) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        assertThat(String.format("metric's (%s) count is not higher than %s", metric, count), values.stream().mapToDouble(i -> i).count() > count);
    }

    private void assertMetricValueHigherThan(MetricsCollector collector, String metric, int value) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        assertThat(String.format("metric's (%s) values is not higher than %s", metric, value), values.stream().mapToDouble(i -> i).sum() > value);
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
        clusterOperator = clusterOperator.defaultInstallation()
            .withWatchingNamespaces(clusterOperator.getDeploymentNamespace() + "," + SECOND_NAMESPACE)
            .withBindingsNamespaces(Arrays.asList(clusterOperator.getDeploymentNamespace(), SECOND_NAMESPACE))
            .createInstallation()
            .runInstallation();

        final String scraperName = clusterOperator.getDeploymentNamespace() + "-" + Constants.SCRAPER_NAME;
        final String secondScraperName = SECOND_NAMESPACE + "-" + Constants.SCRAPER_NAME;

        cluster.setNamespace(clusterOperator.getDeploymentNamespace());

        // create resources without wait to deploy them simultaneously
        resourceManager.createResource(extensionContext, false,
            // kafka with cruise control and metrics
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(clusterOperator.getDeploymentNamespace(), metricsClusterName, 3, 3)
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
            KafkaTemplates.kafkaWithMetrics(SECOND_CLUSTER, SECOND_NAMESPACE, 1, 1).build(),
            ScraperTemplates.scraperPod(clusterOperator.getDeploymentNamespace(), scraperName).build(),
            ScraperTemplates.scraperPod(SECOND_NAMESPACE, secondScraperName).build()
        );

        // sync resources
        resourceManager.synchronizeResources(extensionContext);

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridgeWithMetrics(BRIDGE_CLUSTER, metricsClusterName, KafkaResources.plainBootstrapAddress(metricsClusterName), 1).build());
        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2WithMetrics(clusterOperator.getDeploymentNamespace(), MIRROR_MAKER_CLUSTER, metricsClusterName, SECOND_CLUSTER, 1, SECOND_NAMESPACE, clusterOperator.getDeploymentNamespace()).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(metricsClusterName, topicName, 7, 2).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(metricsClusterName, kafkaExporterTopic, 7, 2).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(metricsClusterName, bridgeTopic).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterOperator.getDeploymentNamespace(), metricsClusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterOperator.getDeploymentNamespace(), metricsClusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());
        resourceManager.createResource(extensionContext,
                KafkaConnectTemplates.kafkaConnectWithMetricsAndFileSinkPlugin(metricsClusterName, clusterOperator.getDeploymentNamespace(), metricsClusterName, 1)
                        .editMetadata()
                        .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                        .endMetadata()
                        .build());
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(metricsClusterName).build());

        scraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), scraperName).get(0).getMetadata().getName();
        secondNamespaceScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(SECOND_NAMESPACE, secondScraperName).get(0).getMetadata().getName();

        // Allow connections from clients to operators pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(extensionContext, clusterOperator.getDeploymentNamespace());
        NetworkPolicyResource.allowNetworkPolicySettingsForEntityOperator(extensionContext, metricsClusterName, clusterOperator.getDeploymentNamespace());
        NetworkPolicyResource.allowNetworkPolicySettingsForEntityOperator(extensionContext, SECOND_CLUSTER, SECOND_NAMESPACE);
        // Allow connections from clients to KafkaExporter when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, metricsClusterName, clusterOperator.getDeploymentNamespace());
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, SECOND_CLUSTER, SECOND_NAMESPACE);

        // wait some time for metrics to be stable - at least reconciliation interval + 10s
        Thread.sleep(Constants.SAFETY_RECONCILIATION_INTERVAL);

        kafkaCollector = new MetricsCollector.Builder()
            .withScraperPodName(scraperPodName)
            .withComponentType(ComponentType.Kafka)
            .withComponentName(metricsClusterName)
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

        kafkaConnectCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.KafkaConnect)
            .build();

        clusterOperatorCollector = kafkaCollector.toBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withComponentType(ComponentType.ClusterOperator)
            .withComponentName("")
            .build();

        kafkaCollector.collectMetricsFromPods();
        kafkaExporterCollector.collectMetricsFromPods();
        kafkaConnectCollector.collectMetricsFromPods();
        clusterOperatorCollector.collectMetricsFromPods();
    }

    private List<String> getExpectedTopics() {
        ArrayList<String> list = new ArrayList<>();
        list.add("mirrormaker2-cluster-configs");
        list.add("mirrormaker2-cluster-offsets");
        list.add("mirrormaker2-cluster-status");
        list.add("mm2-offset-syncs.my-cluster.internal");
        list.add(metricsClusterName + "-connect-config");
        list.add(metricsClusterName + "-connect-offsets");
        list.add(metricsClusterName + "-connect-status");
        list.add("heartbeats");
        list.add(topicName);
        list.add(bridgeTopic);
        list.add(kafkaExporterTopic);
        list.add(CruiseControlUtils.CRUISE_CONTROL_METRICS_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        list.add("strimzi-store-topic");
        list.add("strimzi-topic-operator-kstreams-topic-store-changelog");
        list.add("__consumer-offsets---hash");
        return list;
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSourceBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.Constants.SMOKE;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@IsolatedSuite
class ConnectIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectIsolatedST.class);

    @ParallelNamespaceTest
    void testDeployUndeploy(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        final Map<String, Object> exceptedConfig = StUtils.loadProperties("group.id=" + KafkaConnectResources.deploymentName(storage.getClusterName()) + "\n" +
                "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "config.storage.replication.factor=-1\n" +
                "offset.storage.replication.factor=-1\n" +
                "status.storage.replication.factor=-1\n" +
                "config.storage.topic=" + KafkaConnectResources.metricsAndLogConfigMapName(storage.getClusterName()) + "\n" +
                "status.storage.topic=" + KafkaConnectResources.configStorageTopicStatus(storage.getClusterName()) + "\n" +
                "offset.storage.topic=" + KafkaConnectResources.configStorageTopicOffsets(storage.getClusterName()) + "\n");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1).build());

        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");

        final String podName = PodUtils.getPodNameByPrefix(storage.getNamespaceName(), KafkaConnectResources.deploymentName(storage.getClusterName()));
        final String kafkaPodJson = TestUtils.toJsonString(kubeClient(storage.getNamespaceName()).getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.tlsBootstrapAddress(storage.getClusterName()))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(exceptedConfig));
        testDockerImagesForKafkaConnect(INFRA_NAMESPACE, storage.getNamespaceName(), storage.getClusterName());

        verifyLabelsOnPods(storage.getNamespaceName(), storage.getClusterName(), "connect", "KafkaConnect");
        verifyLabelsForService(storage.getNamespaceName(), storage.getClusterName(), "connect-api", "KafkaConnect");
        verifyLabelsForConfigMaps(storage.getNamespaceName(), storage.getClusterName(), null, "");
        verifyLabelsForServiceAccounts(storage.getNamespaceName(), storage.getClusterName(), null);
    }

    private void testDockerImagesForKafkaConnect(String clusterOperatorNamespace, String connectNamespaceName, String clusterName) {
        LOGGER.info("Verifying docker image names");
        Map<String, String> imgFromDeplConf = getImagesFromConfig(clusterOperatorNamespace);
        //Verifying docker image for kafka connect
        String connectImageName = PodUtils.getFirstContainerImageNameFromPod(connectNamespaceName, kubeClient(connectNamespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(kubeClient(connectNamespaceName).getClient()).inNamespace(connectNamespaceName).withName(clusterName).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), is(connectImageName));
        LOGGER.info("Docker images verified");
    }

    @ParallelNamespaceTest
    @Tag(SMOKE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectAndPausedConnectorWithFileSinkPlugin(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(storage.getClusterName(), storage.getTopicName()).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        final String kafkaConnectPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.deploymentName(storage.getClusterName())).get(0).getMetadata().getName();
        final String scraperPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(storage.getScraperName()).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(storage.getNamespaceName(), kafkaConnectPodName);

        LOGGER.info("Creating KafkaConnector with 'pause: true'");

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(storage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", storage.getTopicName())
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(storage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(storage.getClusterName()))
            .withProducerName(storage.getProducerName())
            .withConsumerName(storage.getConsumerName())
            .withNamespaceName(storage.getNamespaceName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(storage.getProducerName(), storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        LOGGER.info("Pausing KafkaConnector: {}", storage.getClusterName());
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(storage.getClusterName(),
            kafkaConnector -> kafkaConnector.getSpec().setPause(true), storage.getNamespaceName());

        KafkaConnectorUtils.waitForConnectorReady(storage.getNamespaceName(), storage.getClusterName());

        LOGGER.info("Clearing FileSink file to check if KafkaConnector will be really paused");
        KafkaConnectUtils.clearFileSinkFile(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH);

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(storage.getProducerName(), storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        LOGGER.info("Because KafkaConnector is paused, no messages should appear to FileSink file");
        assertThrows(Exception.class, () -> KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99"));

        LOGGER.info("Unpausing KafkaConnector, messages should again appear to FileSink file");
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(storage.getClusterName(),
            kafkaConnector -> kafkaConnector.getSpec().setPause(false), storage.getNamespaceName());

        KafkaConnectorUtils.waitForConnectorReady(storage.getNamespaceName(), storage.getClusterName());

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectWithPlainAndScramShaAuthentication(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser =  KafkaUserTemplates.scramShaUser(storage.getClusterName(), storage.getUserName()).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(storage.getClusterName(), storage.getTopicName()).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
            .withNewSpec()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(storage.getClusterName()))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(storage.getUserName())
                    .withPasswordSecret(new PasswordSecretSourceBuilder()
                        .withSecretName(storage.getUserName())
                        .withPassword("password")
                        .build())
                .endKafkaClientAuthenticationScramSha512()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(1)
            .endSpec()
            .build());

        final String kafkaConnectPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.deploymentName(storage.getClusterName())).get(0).getMetadata().getName();
        final String kafkaConnectLogs = kubeClient(storage.getNamespaceName()).logs(kafkaConnectPodName);
        final String scraperPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(storage.getScraperName()).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(storage.getNamespaceName(), kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", scraperPodName, storage.getTopicName());
        KafkaConnectorUtils.createFileSinkConnector(storage.getNamespaceName(), scraperPodName, storage.getTopicName(),
            Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(storage.getClusterName(), storage.getNamespaceName(), 8083));

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(storage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withUserName(storage.getUserName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(storage.getClusterName()))
            .withProducerName(storage.getProducerName())
            .withConsumerName(storage.getConsumerName())
            .withNamespaceName(storage.getNamespaceName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerScramShaPlainStrimzi(),
            kafkaClients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForClientsSuccess(storage.getProducerName(), storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectAndConnectorFileSinkPlugin(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                .endSpec()
            .build());

        String connectorName = "license-source";

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectorName, storage.getClusterName(), 2)
            .editSpec()
                .addToConfig("topic", storage.getTopicName())
            .endSpec()
            .build());

        final String scraperPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(storage.getScraperName()).get(0).getMetadata().getName();

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(storage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(storage.getClusterName()))
            .withConsumerName(storage.getConsumerName())
            .withNamespaceName(storage.getNamespaceName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        String service = KafkaConnectResources.url(storage.getClusterName(), storage.getNamespaceName(), 8083);
        String output = cmdKubeClient(storage.getNamespaceName()).execInPod(scraperPodName, "/bin/bash", "-c", "curl " + service + "/connectors/" + connectorName).out();
        assertThat(output, containsString("\"name\":\"license-source\""));
        assertThat(output, containsString("\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\""));
        assertThat(output, containsString("\"tasks.max\":\"2\""));
        assertThat(output, containsString("\"topic\":\"" + storage.getTopicName() + "\""));
    }


    @ParallelNamespaceTest
    void testJvmAndResources(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("400M"))
                    .addToLimits("cpu", new Quantity("2"))
                    .addToRequests("memory", new Quantity("300M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build())
                .withNewJvmOptions()
                    .withXmx("200m")
                    .withXms("200m")
                    .withXx(jvmOptionsXX)
                .endJvmOptions()
            .endSpec()
            .build());

        String podName = PodUtils.getPodNameByPrefix(namespaceName, KafkaConnectResources.deploymentName(clusterName));
        assertResources(namespaceName, podName, KafkaConnectResources.deploymentName(clusterName),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(namespaceName, podName, KafkaConnectResources.deploymentName(clusterName),
                "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    void testKafkaConnectScaleUpScaleDown(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        LOGGER.info("Running kafkaConnectScaleUP {} in namespace", namespaceName);

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false).build());

        String deploymentName = KafkaConnectResources.deploymentName(clusterName);

        // kafka cluster Connect already deployed
        List<Pod> connectPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));
        final int scaleTo = initialReplicas + 3;

        LOGGER.info("Scaling up to {}", scaleTo);
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, c -> c.getSpec().setReplicas(scaleTo), namespaceName);

        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, deploymentName, scaleTo);
        connectPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        assertThat(connectPods.size(), is(scaleTo));

        LOGGER.info("Scaling down to {}", initialReplicas);
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, c -> c.getSpec().setReplicas(initialReplicas), namespaceName);

        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, deploymentName, initialReplicas);
        connectPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        assertThat(connectPods.size(), is(initialReplicas));
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser = KafkaUserTemplates.tlsUser(storage.getClusterName(), storage.getUserName()).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(storage.getClusterName(), storage.getTopicName()).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                .addNewTrustedCertificate()
                    .withSecretName(storage.getClusterName() + "-cluster-ca-cert")
                    .withCertificate("ca.crt")
                .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(storage.getClusterName() + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(storage.getUserName())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endSpec()
                .build());

        final String kafkaConnectPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.deploymentName(storage.getClusterName())).get(0).getMetadata().getName();
        final String kafkaConnectLogs = kubeClient(storage.getNamespaceName()).logs(kafkaConnectPodName);
        final String scraperPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(storage.getScraperName()).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(storage.getNamespaceName(), kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", scraperPodName, storage.getTopicName());
        KafkaConnectorUtils.createFileSinkConnector(storage.getNamespaceName(), scraperPodName, storage.getTopicName(),
            Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(storage.getClusterName(), storage.getNamespaceName(), 8083));

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(storage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withUserName(storage.getUserName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(storage.getClusterName()))
            .withProducerName(storage.getProducerName())
            .withConsumerName(storage.getConsumerName())
            .withNamespaceName(storage.getNamespaceName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(storage.getClusterName()), kafkaClients.consumerTlsStrimzi(storage.getClusterName()));
        ClientUtils.waitForClientsSuccess(storage.getProducerName(), storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(storage.getClusterName(), storage.getUserName()).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(storage.getClusterName(), storage.getTopicName()).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(storage.getClusterName() + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(storage.getClusterName() + "-kafka-bootstrap:9093")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(storage.getUserName())
                    .withNewPasswordSecret()
                        .withSecretName(storage.getUserName())
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
            .endSpec()
            .build());

        final String kafkaConnectPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.deploymentName(storage.getClusterName())).get(0).getMetadata().getName();
        final String kafkaConnectLogs = kubeClient(storage.getNamespaceName()).logs(kafkaConnectPodName);
        final String scraperPodName = kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(storage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", scraperPodName, storage.getTopicName());
        KafkaConnectorUtils.createFileSinkConnector(storage.getNamespaceName(), scraperPodName, storage.getTopicName(),
            Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(storage.getClusterName(), storage.getNamespaceName(), 8083));

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(storage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(storage.getClusterName()))
            .withProducerName(storage.getProducerName())
            .withConsumerName(storage.getConsumerName())
            .withNamespaceName(storage.getNamespaceName())
            .withUserName(storage.getUserName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerScramShaTlsStrimzi(storage.getClusterName()), kafkaClients.consumerScramShaTlsStrimzi(storage.getClusterName()));
        ClientUtils.waitForClientsSuccess(storage.getProducerName(), storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String usedVariable = "KAFKA_CONNECT_CONFIGURATION";

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");
        envVarGeneral.put(usedVariable, "test.value");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, Object> connectConfig = new HashMap<>();
        connectConfig.put("config.storage.replication.factor", "-1");
        connectConfig.put("offset.storage.replication.factor", "-1");
        connectConfig.put("status.storage.replication.factor", "-1");

        final int initialDelaySeconds = 30;
        final int timeoutSeconds = 10;
        final int updatedInitialDelaySeconds = 31;
        final int updatedTimeoutSeconds = 11;
        final int periodSeconds = 10;
        final int successThreshold = 1;
        final int failureThreshold = 3;
        final int updatedPeriodSeconds = 5;
        final int updatedFailureThreshold = 1;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .editSpec()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endConnectContainer()
                .endTemplate()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endLivenessProbe()
            .endSpec()
            .build());

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaConnectResources.deploymentName(clusterName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(namespaceName, KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(namespaceName, kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName(), usedVariable),
                is(not("test.value"))
        );

        LOGGER.info("Updating values in MirrorMaker container");

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, kc -> {
            kc.getSpec().getTemplate().getConnectContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kc.getSpec().setConfig(connectConfig);
            kc.getSpec().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kc.getSpec().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kc.getSpec().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kc.getSpec().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kc.getSpec().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kc.getSpec().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kc.getSpec().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kc.getSpec().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
        }, namespaceName);

        DeploymentUtils.waitTillDepHasRolled(namespaceName, KafkaConnectResources.deploymentName(clusterName), 1, connectSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(namespaceName, KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), envVarUpdated);
        checkComponentConfiguration(namespaceName, KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), "KAFKA_CONNECT_CONFIGURATION", connectConfig);
    }

    @ParallelNamespaceTest
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ACCEPTANCE)
    void testMultiNodeKafkaConnectWithConnectorCreation(ExtensionContext extensionContext) {
        TestStorage storage = new TestStorage(extensionContext);

        final String connectClusterName = storage.getClusterName() + "-2";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 3).build());
        // Crate connect cluster with default connect image
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 3)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("group.id", connectClusterName)
                .addToConfig("offset.storage.topic", connectClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectClusterName + "-config")
                .addToConfig("status.storage.topic", connectClusterName + "-status")
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(storage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", storage.getTopicName())
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(storage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(storage.getClusterName()))
            .withProducerName(storage.getProducerName())
            .withConsumerName(storage.getConsumerName())
            .withNamespaceName(storage.getNamespaceName())
            .build();

        String execConnectPod =  kubeClient(storage.getNamespaceName()).listPodsByPrefixInName(
            KafkaConnectResources.deploymentName(storage.getClusterName())).get(0).getMetadata().getName();
        JsonObject connectStatus = new JsonObject(cmdKubeClient(storage.getNamespaceName()).execInPod(
                execConnectPod,
                "curl", "-X", "GET", "http://localhost:8083/connectors/" + storage.getClusterName() + "/status").out()
        );
        String podIP = connectStatus.getJsonObject("connector").getString("worker_id").split(":")[0];
        String connectorPodName = kubeClient(storage.getNamespaceName()).listPods().stream().filter(pod ->
                pod.getStatus().getPodIP().equals(podIP)).findFirst().orElseThrow().getMetadata().getName();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(storage.getProducerName(), storage.getConsumerName(), storage.getNamespaceName(), MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(storage.getNamespaceName(), connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @ParallelNamespaceTest
    void testConnectTlsAuthWithWeirdUserName(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationTls())
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationTls())
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, weirdUserName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder()
                        .withCertificate("ca.crt")
                        .withSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                        .build())
                .endTls()
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(weirdUserName)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
            .endSpec()
            .build());

        testConnectAuthorizationWithWeirdUserName(extensionContext, clusterName, weirdUserName, SecurityProtocol.SSL, topicName);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @ParallelNamespaceTest
    void testConnectScramShaAuthWithWeirdUserName(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasdsadasdasdasdasdgasgadfasdad";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, weirdUserName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(weirdUserName)
                        .withPasswordSecret(new PasswordSecretSourceBuilder()
                            .withSecretName(weirdUserName)
                            .withPassword("password")
                            .build())
                    .endKafkaClientAuthenticationScramSha512()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                                .build())
                    .endTls()
                .endSpec()
                .build());

        testConnectAuthorizationWithWeirdUserName(extensionContext, clusterName, weirdUserName, SecurityProtocol.SASL_SSL, topicName);
    }

    void testConnectAuthorizationWithWeirdUserName(ExtensionContext extensionContext, String clusterName, String userName, SecurityProtocol securityProtocol, String topicName) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String connectorPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-connect").get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
            .endSpec()
            .build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(securityProtocol)
            .withTopicName(topicName)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(externalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(namespaceName, connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "\"Hello-world - 99\"");
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    void testScaleConnectWithoutConnectorToZero(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 2, false).build());

        final String connectDeploymentName = KafkaConnectResources.deploymentName(clusterName);
        List<Pod> connectPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));

        assertThat(connectPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, kafkaConnect -> kafkaConnect.getSpec().setReplicas(0), namespaceName);

        KafkaConnectUtils.waitForConnectReady(namespaceName, clusterName);
        PodUtils.waitForPodsReady(kubeClient(namespaceName).getDeploymentSelectors(namespaceName, connectDeploymentName), 0, true);

        connectPods = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaConnectResources.deploymentName(clusterName));
        KafkaConnectStatus connectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();

        assertThat(connectPods.size(), is(0));
        assertThat(connectStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    @Tag(CONNECTOR_OPERATOR)
    void testScaleConnectWithConnectorToZero(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 2, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        String connectDeploymentName = KafkaConnectResources.deploymentName(clusterName);
        List<Pod> connectPods = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaConnectResources.deploymentName(clusterName));

        assertThat(connectPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, kafkaConnect -> kafkaConnect.getSpec().setReplicas(0), namespaceName);

        KafkaConnectUtils.waitForConnectReady(namespaceName, clusterName);
        PodUtils.waitForPodsReady(namespaceName, kubeClient(namespaceName).getDeploymentSelectors(namespaceName, connectDeploymentName), 0, true);

        connectPods = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, connectDeploymentName);
        KafkaConnectStatus connectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();
        KafkaConnectorStatus connectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();

        assertThat(connectPods.size(), is(0));
        assertThat(connectStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getMessage().contains("has 0 replicas")), is(true));
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    @Tag(CONNECTOR_OPERATOR)
    void testScaleConnectAndConnectorSubresource(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        final int scaleTo = 4;
        final long connectObsGen = KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();
        final String connectGenName = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaConnect subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient(namespaceName).scaleByName(KafkaConnect.RESOURCE_KIND, clusterName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaConnectResources.deploymentName(clusterName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, observed generation is higher - for spec and status - naming prefix should be same", scaleTo);
        List<Pod> connectPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        assertThat(connectPods.size(), is(4));
        assertThat(KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(connectObsGen < KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration(), is(true));
        for (Pod pod : connectPods) {
            assertThat(pod.getMetadata().getName().contains(connectGenName), is(true));
        }

        LOGGER.info("-------> Scaling KafkaConnector subresource <-------");
        LOGGER.info("Scaling subresource task max to {}", scaleTo);
        cmdKubeClient(namespaceName).scaleByName(KafkaConnector.RESOURCE_KIND, clusterName, scaleTo);
        KafkaConnectorUtils.waitForConnectorsTaskMaxChange(namespaceName, clusterName, scaleTo);

        LOGGER.info("Check if taskMax is set to {}", scaleTo);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getTasksMax(), is(scaleTo));
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getTasksMax(), is(scaleTo));

        LOGGER.info("Check taskMax on Connect pods API");
        for (Pod pod : connectPods) {
            JsonObject json = new JsonObject(KafkaConnectorUtils.getConnectorSpecFromConnectAPI(namespaceName, pod.getMetadata().getName(), clusterName));
            assertThat(Integer.parseInt(json.getJsonObject("config").getString("tasks.max")), is(scaleTo));
        }
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMountingSecretAndConfigMapAsVolumesAndEnvVars(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        final String secretPassword = "password";
        final String encodedPassword = Base64.getEncoder().encodeToString(secretPassword.getBytes());

        final String secretEnv = "MY_CONNECT_SECRET";
        final String configMapEnv = "MY_CONNECT_CONFIG_MAP";

        final String dotedSecretEnv = "MY_DOTED_CONNECT_SECRET";
        final String dotedConfigMapEnv = "MY_DOTED_CONNECT_CONFIG_MAP";

        final String configMapName = "connect-config-map";
        final String secretName = "connect-secret";

        final String dotedConfigMapName = "connect.config.map";
        final String dotedSecretName = "connect.secret";

        final String configMapKey = "my-key";
        final String secretKey = "my-secret-key";

        final String configMapValue = "my-value";

        Secret connectSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(secretName)
            .endMetadata()
            .withType("Opaque")
            .addToData(secretKey, encodedPassword)
            .build();

        ConfigMap configMap = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(configMapName)
            .endMetadata()
            .addToData(configMapKey, configMapValue)
            .build();

        Secret dotedConnectSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(dotedSecretName)
            .endMetadata()
            .withType("Opaque")
            .addToData(secretKey, encodedPassword)
            .build();

        ConfigMap dotedConfigMap = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(dotedConfigMapName)
            .endMetadata()
            .addToData(configMapKey, configMapValue)
            .build();

        kubeClient(namespaceName).createSecret(connectSecret);
        kubeClient(namespaceName).createSecret(dotedConnectSecret);
        kubeClient(namespaceName).getClient().configMaps().inNamespace(namespaceName).createOrReplace(configMap);
        kubeClient(namespaceName).getClient().configMaps().inNamespace(namespaceName).createOrReplace(dotedConfigMap);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .withNewExternalConfiguration()
                    .addNewVolume()
                        .withName(secretName)
                        .withSecret(new SecretVolumeSourceBuilder().withSecretName(secretName).build())
                    .endVolume()
                    .addNewVolume()
                        .withName(configMapName)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withName(configMapName).build())
                    .endVolume()
                    .addNewVolume()
                        .withName(dotedSecretName)
                        .withSecret(new SecretVolumeSourceBuilder().withSecretName(dotedSecretName).build())
                    .endVolume()
                    .addNewVolume()
                        .withName(dotedConfigMapName)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withName(dotedConfigMapName).build())
                    .endVolume()
                    .addNewEnv()
                        .withName(secretEnv)
                        .withNewValueFrom()
                            .withSecretKeyRef(
                                new SecretKeySelectorBuilder()
                                    .withKey(secretKey)
                                    .withName(connectSecret.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withName(configMapEnv)
                        .withNewValueFrom()
                            .withConfigMapKeyRef(
                                new ConfigMapKeySelectorBuilder()
                                    .withKey(configMapKey)
                                    .withName(configMap.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withName(dotedSecretEnv)
                        .withNewValueFrom()
                            .withSecretKeyRef(
                                new SecretKeySelectorBuilder()
                                    .withKey(secretKey)
                                    .withName(dotedConnectSecret.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withName(dotedConfigMapEnv)
                        .withNewValueFrom()
                            .withConfigMapKeyRef(
                                new ConfigMapKeySelectorBuilder()
                                    .withKey(configMapKey)
                                    .withName(dotedConfigMap.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                .endExternalConfiguration()
            .endSpec()
            .build());

        String connectPodName = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        LOGGER.info("Check if the ENVs contains desired values");
        assertThat(cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + secretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + configMapEnv).out().trim(), equalTo(configMapValue));
        assertThat(cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + dotedSecretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + dotedConfigMapEnv).out().trim(), equalTo(configMapValue));

        LOGGER.info("Check if volumes contains desired values");
        assertThat(
            cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + configMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + secretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
        assertThat(
            cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedConfigMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedSecretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
    }

    @ParallelNamespaceTest
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String connectDepName = KafkaConnectResources.deploymentName(clusterName);

        LOGGER.info("Adding label to Connect resource, the CR should be recreated");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap("some", "label")), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, connectDepName, 1);

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaConnect.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaConnect.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaConnect.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName,
            kc -> kc.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), namespaceName);
        KafkaConnectUtils.waitForConnectReady(namespaceName, clusterName);

        LOGGER.info("Adding another label to Connect resource, pods should be rolled");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, kc -> kc.getMetadata().getLabels().put("another", "label"), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, connectDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get();
        assertThat(kafkaConnect.getStatus().getObservedGeneration(), is(2L));
        assertThat(kafkaConnect.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kafkaConnect.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    // changing the password in secret should cause the RU of connect pod
    void testKafkaConnectWithScramShaAuthenticationRolledAfterPasswordChanged(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .build())
                .endKafka()
                .endSpec()
                .build());

        Secret passwordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("custom-pwd-secret")
                .endMetadata()
                .addToData("pwd", "MTIzNDU2Nzg5")
                .build();

        kubeClient(namespaceName).createSecret(passwordSecret);

        KafkaUser kafkaUser =  KafkaUserTemplates.scramShaUser(clusterName, userName)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("pwd", "custom-pwd-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        resourceManager.createResource(extensionContext, kafkaUser);

        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, userName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
                .withNewSpec()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(clusterName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(userName)
                        .withPasswordSecret(new PasswordSecretSourceBuilder()
                            .withSecretName(userName)
                            .withPassword("password")
                            .build())
                    .endKafkaClientAuthenticationScramSha512()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withVersion(Environment.ST_KAFKA_VERSION)
                    .withReplicas(1)
                .endSpec()
                .build());

        final String kafkaConnectPodName = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(namespaceName, kafkaConnectPodName);

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaConnectResources.deploymentName(clusterName));
        String newPassword = "bmVjb0ppbmVob05lelNwcmF2bnlQYXNzd29yZA==";
        Secret newPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("new-custom-pwd-secret")
                .endMetadata()
                .addToData("pwd", newPassword)
                .build();

        kubeClient(namespaceName).createSecret(newPasswordSecret);

        kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, userName)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("pwd", "new-custom-pwd-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        resourceManager.createResource(extensionContext, kafkaUser);

        DeploymentUtils.waitTillDepHasRolled(namespaceName, KafkaConnectResources.deploymentName(clusterName), 1, connectSnapshot);

        final String kafkaConnectPodNameAfterRU = kubeClient(namespaceName).listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(namespaceName, kafkaConnectPodNameAfterRU);
    }

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}

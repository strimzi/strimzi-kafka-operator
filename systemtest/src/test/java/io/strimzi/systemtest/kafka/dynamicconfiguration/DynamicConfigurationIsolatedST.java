/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.dynamicconfiguration;

import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DynamicConfigurationIsolatedST is responsible for verify that if we change dynamic Kafka configuration it will not
 * trigger rolling update.
 * Isolated -> for each test case we have different configuration of Kafka resource
 */
@Tag(REGRESSION)
@Tag(DYNAMIC_CONFIGURATION)
public class DynamicConfigurationIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationIsolatedST.class);
    private static final String NAMESPACE = "kafka-configuration-isolated-cluster-test";
    private static final int KAFKA_REPLICAS = 3;

    private Map<String, Object> kafkaConfig;

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testSimpleDynamicConfiguration(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        Map<String, Object> deepCopyOfShardKafkaConfig = kafkaConfig.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, KAFKA_REPLICAS, 1)
            .editSpec()
                .editKafka()
                    .withConfig(deepCopyOfShardKafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        String kafkaConfiguration = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName)).getData().get("server.config");
        assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("log.message.format.version=" + TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).messageVersion()));

        String kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", true);

        updateAndVerifyDynConf(clusterName, deepCopyOfShardKafkaConfig);

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=" + true));

        LOGGER.info("Verify values after update");
        kafkaConfiguration = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName)).getData().get("server.config");
        assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("log.message.format.version=" + TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).messageVersion()));
        assertThat(kafkaConfiguration, containsString("unclean.leader.election.enable=true"));
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(ROLLING_UPDATE)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testUpdateToExternalListenerCausesRollingRestart(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        Map<String, Object> deepCopyOfShardKafkaConfig = kafkaConfig.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, KAFKA_REPLICAS, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                        .endGenericKafkaListener()
                    .endListeners()
                    .withConfig(deepCopyOfShardKafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        String kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", true);

        updateAndVerifyDynConf(clusterName, deepCopyOfShardKafkaConfig);

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=" + true));

        // Edit listeners - this should cause RU (because of new crts)
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));
        LOGGER.info("Updating listeners of Kafka cluster");

        KafkaResource.replaceKafkaResource(clusterName, k -> {
            k.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(Arrays.asList(
                new GenericKafkaListenerBuilder()
                    .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(false)
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                    .withPort(9093)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(true)
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(true)
                    .build()
            )));
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), KAFKA_REPLICAS, kafkaPods);
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(clusterName), kafkaPods), is(true));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        deepCopyOfShardKafkaConfig.put("compression.type", "snappy");

        updateAndVerifyDynConf(clusterName, deepCopyOfShardKafkaConfig);

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("compression.type=snappy"));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", true);

        updateAndVerifyDynConf(clusterName, deepCopyOfShardKafkaConfig);

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=" + true));

        // Remove external listeners (node port) - this should cause RU (we need to update advertised.listeners)
        // Other external listeners cases are rolling because of crts
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));
        LOGGER.info("Updating listeners of Kafka cluster");

        KafkaResource.replaceKafkaResource(clusterName, k -> {
            k.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(Arrays.asList(
                new GenericKafkaListenerBuilder()
                    .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(false)
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(true)
                    .build()
            )));
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), KAFKA_REPLICAS, kafkaPods);
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(clusterName), kafkaPods), is(true));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", false);

        updateAndVerifyDynConf(clusterName, deepCopyOfShardKafkaConfig);

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(clusterName, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=" + false));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    void testUpdateToExternalListenerCausesRollingRestartUsingExternalClients(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        Map<String, Object> deepCopyOfShardKafkaConfig = kafkaConfig.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, KAFKA_REPLICAS, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                        .endGenericKafkaListener()
                    .endListeners()
                    .withConfig(deepCopyOfShardKafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName).build());

        BasicExternalKafkaClient basicExternalKafkaClientTls = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        BasicExternalKafkaClient basicExternalKafkaClientPlain = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClientPlain.verifyProducedAndConsumedMessages(
            basicExternalKafkaClientPlain.sendMessagesPlain(),
            basicExternalKafkaClientPlain.receiveMessagesPlain()
        );

        assertThrows(Exception.class, () -> {
            basicExternalKafkaClientTls.sendMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            basicExternalKafkaClientTls.receiveMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to plain communication");
        });

        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResource(clusterName, k -> {
            k.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(Arrays.asList(
                new GenericKafkaListenerBuilder()
                    .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                    .withPort(9093)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(true)
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(true)
                    .withNewKafkaListenerAuthenticationTlsAuth()
                    .endKafkaListenerAuthenticationTlsAuth()
                    .build()
            )));
        });

        // TODO: remove it ?
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), KAFKA_REPLICAS, kafkaPods);

        basicExternalKafkaClientTls.verifyProducedAndConsumedMessages(
                basicExternalKafkaClientTls.sendMessagesTls(),
                basicExternalKafkaClientTls.receiveMessagesTls()
        );

        assertThrows(Exception.class, () -> {
            basicExternalKafkaClientPlain.sendMessagesPlain(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            basicExternalKafkaClientPlain.receiveMessagesPlain(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to tls communication");
        });

        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResource(clusterName, k -> {
            k.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(Collections.singletonList(
                new GenericKafkaListenerBuilder()
                    .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(false)
                    .build()
            )));
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), KAFKA_REPLICAS, kafkaPods);

        assertThrows(Exception.class, () -> {
            basicExternalKafkaClientTls.sendMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            basicExternalKafkaClientTls.receiveMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to plain communication");
        });

        basicExternalKafkaClientPlain.verifyProducedAndConsumedMessages(
                basicExternalKafkaClientPlain.sendMessagesPlain(),
                basicExternalKafkaClientPlain.receiveMessagesPlain()
        );
    }

    /**
     * UpdateAndVerifyDynConf, change the kafka configuration and verify that no rolling update were triggered
     * @param kafkaConfig specific kafka configuration, which will be changed
     */
    private void updateAndVerifyDynConf(String clusterName, Map<String, Object> kafkaConfig) {
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));

        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResource(clusterName, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(kafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(clusterName));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(clusterName), kafkaPods), is(false));
    }

    @BeforeEach
    void setupEach() {
        kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("log.message.format.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).messageVersion());
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
    }
}

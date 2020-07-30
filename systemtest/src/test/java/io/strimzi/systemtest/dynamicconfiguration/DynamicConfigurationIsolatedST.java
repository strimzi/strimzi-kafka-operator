/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.dynamicconfiguration;

import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.InlineLoggingBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DynamicConfigurationIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationIsolatedST.class);
    private static final String NAMESPACE = "kafka-configuration-isolated-cluster-test";

    @Test
    void testSimpleDynamicConfiguration() {
        int kafkaReplicas = 2;
        Map<String, Object> kafkaConfig = new HashMap<>();

        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");
        kafkaConfig.put("log.message.format.version", "2.4");

        Map<String, Object> updatedKafkaConfig = new HashMap<>();
        updatedKafkaConfig.put("offsets.topic.replication.factor", "1");
        updatedKafkaConfig.put("transaction.state.log.replication.factor", "1");
        updatedKafkaConfig.put("default.replication.factor", "1");
        updatedKafkaConfig.put("log.message.format.version", "2.4");
        updatedKafkaConfig.put("unclean.leader.election.enable", "true");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, kafkaReplicas, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(kafkaConfig)
                    .endKafka()
                .endSpec()
                .done();

        String kafkaConfiguration = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME)).getData().get("server.config");
        assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("default.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("log.message.format.version=2.4"));

        String kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, is("Dynamic configs for broker 0 are:\n"));

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(updatedKafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(false));

        LOGGER.info("Verify values after update");
        kafkaConfiguration = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME)).getData().get("server.config");
        assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("default.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("log.message.format.version=2.4"));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=true"));

        InlineLogging il = new InlineLoggingBuilder().withLoggers(Collections.singletonMap("kafka.logger.level", "INFO")).build();

        Map<String, String> kafkaPodsSnapshot = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Updating logging of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setLogging(il);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPodsSnapshot);
    }

    @Test
    void testDynamicConfigurationWithExternalListeners() {
        int kafkaReplicas = 2;
        int zkReplicas = 1;
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");
        kafkaConfig.put("log.message.format.version", "2.4");

        Map<String, Object> updatedKafkaConfig = new HashMap<>();
        updatedKafkaConfig.put("offsets.topic.replication.factor", "1");
        updatedKafkaConfig.put("transaction.state.log.replication.factor", "1");
        updatedKafkaConfig.put("default.replication.factor", "1");
        updatedKafkaConfig.put("log.message.format.version", "2.4");
        updatedKafkaConfig.put("unclean.leader.election.enable", "true");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, kafkaReplicas, zkReplicas)
                .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                        .endKafkaListenerExternalLoadBalancer()
                        .withNewPlain()
                        .endPlain()
                    .endListeners()
                    .withConfig(kafkaConfig)
                .endKafka()
                .endSpec()
                .done();


        // change dynamically changeable option
        updatedKafkaConfig.put("unclean.leader.election.enable", "false");
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(updatedKafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(false));

        String kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        // Edit listeners - this should cause RU (because of new crts)
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            KafkaListeners kl = new KafkaListenersBuilder()
                    .withNewKafkaListenerExternalNodePort()
                    .endKafkaListenerExternalNodePort()
                    .withNewPlain()
                    .endPlain()
                    .build();
            kafkaClusterSpec.setListeners(kl);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(true));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        // change dynamically changeable option
        updatedKafkaConfig.put("unclean.leader.election.enable", "true");
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(updatedKafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(false));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=true"));

        // change dynamically changeable option
        updatedKafkaConfig.put("unclean.leader.election.enable", "false");
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(updatedKafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(false));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=false"));

        // Remove external listeners (node port) - this should cause RU (we need to update advertised.listeners)
        // Other external listeners cases are rolling because of crts
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            KafkaListeners kl = new KafkaListenersBuilder()
                    .withNewPlain()
                    .endPlain()
                    .build();
            kafkaClusterSpec.setListeners(kl);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(true));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));


        // change dynamically changeable option
        updatedKafkaConfig.put("unclean.leader.election.enable", "true");
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(updatedKafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(StatefulSetUtils.ssHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods), is(false));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();
        assertThat(kafkaConfigurationFromPod, containsString("unclean.leader.election.enable=true"));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testDynamicConfigurationExternalTls() {
        int kafkaReplicas = 2;
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");
        kafkaConfig.put("log.message.format.version", "2.4");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, kafkaReplicas, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalLoadBalancer()
                                .withTls(false)
                            .endKafkaListenerExternalLoadBalancer()
                        .endListeners()
                        .withConfig(kafkaConfig)
                    .endKafka()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        BasicExternalKafkaClient basicExternalKafkaClientTls = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .withSecurityProtocol(SecurityProtocol.SSL)
            .build();

        BasicExternalKafkaClient basicExternalKafkaClientPlain = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .build();

        String userName = "john";
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        basicExternalKafkaClientTls.setKafkaUsername(userName);

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
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaListeners updatedKl = new KafkaListenersBuilder()
                    .withNewKafkaListenerExternalNodePort()
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                    .endKafkaListenerExternalNodePort()
                    .build();
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setListeners(updatedKl);
        });

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        basicExternalKafkaClientTls.verifyProducedAndConsumedMessages(
                basicExternalKafkaClientTls.sendMessagesTls(),
                basicExternalKafkaClientTls.sendMessagesTls()
        );

        assertThrows(Exception.class, () -> {
            basicExternalKafkaClientPlain.sendMessagesPlain(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            basicExternalKafkaClientPlain.receiveMessagesPlain(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT);
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to tls communication");
        });

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaListeners updatedKl = new KafkaListenersBuilder()
                    .withNewKafkaListenerExternalNodePort()
                        .withTls(false)
                    .endKafkaListenerExternalNodePort()
                    .build();
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setListeners(updatedKl);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);

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

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
    }
}

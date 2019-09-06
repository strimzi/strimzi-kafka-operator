/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.test.BaseITST;
import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.util.Properties;
import java.util.stream.Collectors;

@RunWith(VertxUnitRunner.class)
public class TopicOperatorTopicDeletionDisabledIT extends TopicOperatorIT {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorTopicDeletionDisabledIT.class);

    @Before
    public void setup(TestContext context) throws Exception {
        LOGGER.info("Setting up test");
        kubeCluster().before();
        Runtime.getRuntime().addShutdownHook(kafkaHook);
        int counts = 3;
        do {
            try {
                kafkaCluster = new KafkaCluster();
                kafkaCluster.addBrokers(1);
                Properties props = new Properties();
                props.setProperty("delete.topic.enable", "false");
                kafkaCluster.withKafkaConfiguration(props);
                kafkaCluster.deleteDataPriorToStartup(true);
                kafkaCluster.deleteDataUponShutdown(true);
                kafkaCluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
                kafkaCluster.startup();
                break;
            } catch (kafka.zookeeper.ZooKeeperClientTimeoutException e) {
                if (counts == 0) {
                    throw e;
                }
                counts--;
            }
        } while (true);

        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.brokerList());
        adminClient = AdminClient.create(p);

        kubeClient = BaseITST.kubeClient().getClient();
        Crds.registerCustomKinds();
        LOGGER.info("Using namespace {}", NAMESPACE);
        startTopicOperator(context);

        // We can't delete events, so record the events which exist at the start of the test
        // and then waitForEvents() can ignore those
        preExistingEvents = kubeClient.events().inNamespace(NAMESPACE).withLabels(labels.labels()).list().
                getItems().stream().
                map(evt -> evt.getMetadata().getUid()).
                collect(Collectors.toSet());

        LOGGER.info("Finished setting up test");
    }

    @After
    public void teardown(TestContext context) {
        LOGGER.info("Tearing down test");

        if (kubeClient != null) {
            operation().inNamespace(NAMESPACE).delete();
        }

        stopTopicOperator(context);

        adminClient.close();
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        Runtime.getRuntime().removeShutdownHook(kafkaHook);
        LOGGER.info("Finished tearing down test");
    }

    @Test
    public void testKafkaTopicDeletionDisabled(TestContext context) {
        // create the Topic Resource
        String topicName = "test-topic-deletion-disabled";
        // The creation method will wait for the topic to be ready in K8s
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        // Wait for the topic to be ready on the Kafka Broker
        waitForTopicInKafka(context, topicName, true);

        // Delete the k8 KafkaTopic and wait for that to be deleted
        deleteInKube(topicResource.getMetadata().getName());

        // trigger an immediate reconcile where, with with delete.topic.enable=false, the K8s KafkaTopic should be recreated
        Future<?> result = session.topicOperator.reconcileAllTopics("periodic");
        do {
            if (result.isComplete()) {
                break;
            }
        } while (true);

        // Wait for the KafkaTopic to be recreated
        waitForTopicInKube(context, topicName, true);
    }
}


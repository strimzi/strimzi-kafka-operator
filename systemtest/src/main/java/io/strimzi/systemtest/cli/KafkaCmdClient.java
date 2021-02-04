/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cli;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class KafkaCmdClient {

    private static final int PORT = 9092;

    public KafkaCmdClient() { }

    public static List<String> listTopicsUsingPodCli(String clusterName, int kafkaPodId) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return Arrays.asList(cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --list --bootstrap-server localhost:" + PORT).out().split("\\s+"));
    }

    public static String createTopicUsingPodCli(String clusterName, int kafkaPodId, String topic, int replicationFactor, int partitions) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        String response = cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --create " + " --topic " + topic +
                " --replication-factor " + replicationFactor + " --partitions " + partitions).out();

        KafkaTopicUtils.waitForKafkaTopicCreation(topic);

        return response;
    }

    public static String deleteTopicUsingPodCli(String clusterName, int kafkaPodId, String topic) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --delete --topic " + topic).out();
    }

    public static List<String> describeTopicUsingPodCli(String clusterName, int kafkaPodId, String topic) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return Arrays.asList(cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --describe --topic " + topic).out().replace(": ", ":").split("\\s+"));
    }

    public static String updateTopicPartitionsCountUsingPodCli(String clusterName, int kafkaPodId, String topic, int partitions) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --alter --topic " + topic + " --partitions " + partitions).out();
    }
}

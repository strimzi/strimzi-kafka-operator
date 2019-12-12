/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cli;

import io.strimzi.api.kafka.model.KafkaResources;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class KafkaCmdClient {

    public KafkaCmdClient() { }

    public static List<String> listTopicsUsingPodCli(String clusterName, int zkPodId) {
        String podName = KafkaResources.zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return Arrays.asList(cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --list --zookeeper localhost:" + port).out().split("\\s+"));
    }

    public static String createTopicUsingPodCli(String clusterName, int zkPodId, String topic, int replicationFactor, int partitions) {
        String podName = KafkaResources.zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --zookeeper localhost:" + port + " --create " + " --topic " + topic +
                " --replication-factor " + replicationFactor + " --partitions " + partitions).out();
    }

    public static String deleteTopicUsingPodCli(String clusterName, int zkPodId, String topic) {
        String podName = KafkaResources.zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --zookeeper localhost:" + port + " --delete --topic " + topic).out();
    }

    public static List<String> describeTopicUsingPodCli(String clusterName, int zkPodId, String topic) {
        String podName = KafkaResources.zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return Arrays.asList(cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --zookeeper localhost:" + port + " --describe --topic " + topic).out().split("\\s+"));
    }

    public static String updateTopicPartitionsCountUsingPodCli(String clusterName, int zkPodId, String topic, int partitions) {
        String podName = KafkaResources.zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --zookeeper localhost:" + port + " --alter --topic " + topic + " --partitions " + partitions).out();
    }
}

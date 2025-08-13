/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cli;

import io.skodjob.testframe.resources.KubeResourceManager;

import java.util.Arrays;
import java.util.List;

public class KafkaCmdClient {

    public static List<String> listTopicsUsingPodCli(String namespaceName, String podName, String bootstrapServer) {
        return Arrays.asList(KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --list --bootstrap-server " + bootstrapServer).out().split("\\s+"));
    }

    public static String describeTopicUsingPodCli(final String namespaceName, String podName, String bootstrapServer, String topicName) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/opt/kafka/bin/kafka-topics.sh",
                "--topic",
                topicName,
                "--describe",
                "--bootstrap-server",
                bootstrapServer)
            .out();
    }

    public static String describeUserUsingPodCli(String namespaceName, String podName, String bootstrapServer, String userName) {
        return describeKafkaEntityUsingPodCli(namespaceName, podName, bootstrapServer, "users", userName);
    }

    public static String describeKafkaBrokerLoggersUsingPodCli(String namespaceName, String podName, String bootstrapServer, int podNum) {
        return describeKafkaEntityUsingPodCli(namespaceName, podName, bootstrapServer, "broker-loggers", String.valueOf(podNum));
    }

    public static String describeKafkaBrokerUsingPodCli(String namespaceName, String podName, String bootstrapServer, int podNum) {
        return describeKafkaEntityUsingPodCli(namespaceName, podName, bootstrapServer, "brokers", String.valueOf(podNum));
    }

    public static String describeKafkaEntityUsingPodCli(String namespaceName, String podName, String bootstrapServer, String entityType, String entityName) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server "
                    + bootstrapServer + " --entity-type " + entityType + " --entity-name " + entityName + " --describe").out();
    }

    public static String describeKafkaBrokerDefaultsUsingPodCli(String namespaceName, String podName, String bootstrapServer) {
        return describeKafkaEntityDefaultsUsingPodCli(namespaceName, podName, bootstrapServer, "brokers");
    }

    public static String describeKafkaEntityDefaultsUsingPodCli(String namespaceName, String podName, String bootstrapServer, String entityType) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server "
                + bootstrapServer + " --entity-type " + entityType + " --entity-default --describe").out();
    }

    public static String updateFeatureUsingPodCli(String namespaceName, String podName, String bootstrapServer, String featureName, String featureValue) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c", "bin/kafka-features.sh --bootstrap-server "
                + bootstrapServer + " upgrade --feature " + featureName + "=" + featureValue).out();
    }
}

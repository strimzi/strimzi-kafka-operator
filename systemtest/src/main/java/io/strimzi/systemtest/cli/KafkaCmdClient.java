/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cli;

import org.apache.logging.log4j.Level;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class KafkaCmdClient {

    public static List<String> listTopicsUsingPodCli(String namespaceName, String podName, String bootstrapServer) {
        return Arrays.asList(cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --list --bootstrap-server " + bootstrapServer).out().split("\\s+"));
    }

    public static String listTopicsUsingPodCliWithConfigProperties(String namespaceName, String podName, String bootstrapServer, String properties) {
        cmdKubeClient().namespace(namespaceName).execInPod(Level.TRACE,
            podName,
            "/bin/bash", "-c", "echo \"" + properties + "\" | tee /tmp/config.properties"
        );

        return cmdKubeClient().namespace(namespaceName).execInPod(Level.DEBUG, podName, "/opt/kafka/bin/kafka-topics.sh",
                "--list",
                "--bootstrap-server",
                bootstrapServer,
                "--command-config",
                "/tmp/config.properties")
            .out();
    }

    public static String describeTopicUsingPodCli(final String namespaceName, String podName, String bootstrapServer, String topicName) {
        return cmdKubeClient().namespace(namespaceName).execInPod(podName, "/opt/kafka/bin/kafka-topics.sh",
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
        return cmdKubeClient().namespace(namespaceName).execInPod(Level.DEBUG, podName, "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server "
                    + bootstrapServer + " --entity-type " + entityType + " --entity-name " + entityName + " --describe").out();
    }

    public static int getCurrentOffsets(String namespaceName, String podName, String bootstrapServer, String topicName, String consumerGroup) {
        String offsetOutput = cmdKubeClient().namespace(namespaceName).execInPod(podName, "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--describe",
                "--bootstrap-server",
                bootstrapServer,
                "--group",
                consumerGroup)
            .out()
            .trim();

        String replaced = offsetOutput.replaceAll("\\s\\s+", " ");

        List<String> lines = Arrays.asList(replaced.split("\n"));
        List<String> headers = Arrays.asList(lines.get(0).split(" "));
        List<String> matchingLine = Arrays.asList(lines.stream().filter(line -> line.contains(topicName)).findFirst().get().split(" "));

        Map<String, String> valuesMap = IntStream.range(0, headers.size()).boxed().collect(Collectors.toMap(headers::get, matchingLine::get));


        return Integer.parseInt(valuesMap.get("CURRENT-OFFSET"));
    }
}

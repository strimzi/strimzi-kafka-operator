/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;

public class Utils {

    public static TopicMetadata getTopicMetadata(String topicName, Config config) {
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(2, "host2", 1234);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription(topicName, false, asList(
                new TopicPartitionInfo(0, node0, nodes02, nodes02),
                new TopicPartitionInfo(1, node0, nodes02, nodes02)
        ));
        //org.apache.kafka.clients.admin.Config config = new Config(configs);
        return new TopicMetadata(desc, config);
    }

    public static TopicMetadata getTopicMetadata(Topic kubeTopic) {
        List<Node> nodes = new ArrayList<>();
        for (int nodeId = 0; nodeId < kubeTopic.getNumReplicas(); nodeId++) {
            nodes.add(new Node(nodeId, "localhost", 9092 + nodeId));
        }
        List<TopicPartitionInfo> partitions = new ArrayList<>();
        for (int partitionId = 0; partitionId < kubeTopic.getNumPartitions(); partitionId++) {
            partitions.add(new TopicPartitionInfo(partitionId, nodes.get(0), nodes, nodes));
        }
        List<ConfigEntry> configs = new ArrayList<>();
        for (Map.Entry<String, String> entry: kubeTopic.getConfig().entrySet()) {
            configs.add(new ConfigEntry(entry.getKey(), entry.getValue()));
        }

        return new TopicMetadata(new TopicDescription(kubeTopic.getTopicName().toString(), false,
                partitions), new Config(configs));
    }

    public static String getLatestKafkaVersion() {
        // load {kafka.version} from pom file
        // or we could refactor KafkaVersion to operator-common and use its methods here
        final Properties properties = new Properties();
            String path = Utils.class.getClassLoader().getResource("test.properties").getFile().replace("test-classes", "classes");
        try {
            FileReader f = new FileReader(path);
            properties.load(f);
        } catch (IOException e) {
            System.err.println("Kafka version could not be loaded\n" + e.getMessage());
        }
        return properties.getProperty("kafka.version");
    }

}

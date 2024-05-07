/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.test.executor.ExecResult;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class AdminClient {

    private final String namespaceName;
    private final String podName;
    private final static String CMD = "admin-client";
    private final static ObjectMapper MAPPER = new ObjectMapper();

    public AdminClient(String namespaceName, String podName) {
        this.namespaceName = namespaceName;
        this.podName = podName;
    }

    public String listTopics() {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withListSubCommand();

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String deleteTopicsWithPrefixAndCountFromIndex(String topicPrefix, int topicCount, int fromIndex) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDeleteSubcommand()
            .withTopicPrefix(topicPrefix)
            .withFromIndex(fromIndex)
            .withTopicCount(topicCount);

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String deleteTopicsWithPrefixAndCount(String topicPrefix, int topicCount) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDeleteSubcommand()
            .withTopicPrefix(topicPrefix)
            .withTopicCount(topicCount);

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String deleteTopicsWithPrefix(String topicPrefix) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDeleteSubcommand()
            .withTopicPrefix(topicPrefix)
            .withAll();

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();
    }

    public KafkaTopicDescription describeTopic(String topicName) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDescribeSubcommand()
            .withTopicName(topicName)
            .withOutputJson();

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        KafkaTopicDescription[] descriptions = responseFromJSONExecResult(result, KafkaTopicDescription[].class);
        if (descriptions.length == 0) {
            throw new KafkaAdminException("topic: " + topicName + " is not present");
        }
        return descriptions[0];
    }

    private static <T> T responseFromJSONExecResult(ExecResult result, Class<T> responseType) {
        if (result.returnCode() == 0 && !result.out().isEmpty()) {
            try {
                return MAPPER.readValue(result.out(), responseType);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        throw new KafkaAdminException(result.err());
    }

    public String alterPartitionsForTopicsInRange(String topicPrefix, int topicPartitions, int topicsCount, int fromIndex) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withAlterSubcommand()
            .withTopicCount(topicsCount)
            .withTopicPartitions(topicPartitions)
            .withTopicPrefix(topicPrefix)
            .withFromIndex(fromIndex);

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String createTopics(String topicPrefix, int topicsCount, int topicPartitions, int topicReplicas) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withCreateSubcommand()
            .withTopicCount(topicsCount)
            .withTopicPartitions(topicPartitions)
            .withTopicReplicas(topicReplicas)
            .withTopicPrefix(topicPrefix);

        ExecResult result = cmdKubeClient(namespaceName).execInPod(podName, false, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();
    }

    public void configureFromEnv() {
        cmdKubeClient(namespaceName).execInPod(podName, CMD, "configure", "common", "--from-env");
    }

    static class AdminTopicCommand {
        private final static String TOPIC_SUBCOMMAND = "topic";
        private List<String> command = new ArrayList<>(List.of(CMD, TOPIC_SUBCOMMAND));

        public AdminTopicCommand withCreateSubcommand() {
            this.command.add("create");
            return this;
        }

        public AdminTopicCommand withDescribeSubcommand() {
            this.command.add("describe");
            return this;
        }

        public AdminTopicCommand withOutputJson() {
            this.command.add("--output=json");
            return this;
        }

        public AdminTopicCommand withDeleteSubcommand() {
            this.command.add("delete");
            return this;
        }

        public AdminTopicCommand withAlterSubcommand() {
            this.command.add("alter");
            return this;
        }

        public AdminTopicCommand withListSubCommand() {
            this.command.add("list");
            return this;
        }

        public AdminTopicCommand withTopicPrefix(String topicPrefix) {
            this.command.addAll(List.of("-tpref", topicPrefix));
            return this;
        }

        public AdminTopicCommand withTopicName(String topicName) {
            this.command.addAll(List.of("-t", topicName));
            return this;
        }

        public AdminTopicCommand withTopicPartitions(int topicPartitions) {
            this.command.addAll(List.of("-tp", String.valueOf(topicPartitions)));
            return this;
        }

        public AdminTopicCommand withTopicCount(int topicCount) {
            this.command.addAll(List.of("-tc", String.valueOf(topicCount)));
            return this;
        }

        public AdminTopicCommand withTopicReplicas(int topicReplicas) {
            this.command.addAll(List.of("-trf", String.valueOf(topicReplicas)));
            return this;
        }

        public AdminTopicCommand withFromIndex(int fromIndex) {
            this.command.addAll(List.of("-fi", String.valueOf(fromIndex)));
            return this;
        }

        public AdminTopicCommand withAll() {
            this.command.add("--all");
            return this;
        }

        public String[] getCommand() {
            return this.command.toArray(new String[0]);
        }
    }
}

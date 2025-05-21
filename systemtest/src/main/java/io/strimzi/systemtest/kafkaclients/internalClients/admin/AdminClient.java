/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.skodjob.testframe.executor.ExecResult;
import io.skodjob.testframe.resources.KubeResourceManager;

import java.util.ArrayList;
import java.util.List;

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

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String deleteTopicsWithPrefixAndCountFromIndex(String topicPrefix, int topicCount, int fromIndex) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDeleteSubcommand()
            .withTopicPrefix(topicPrefix)
            .withFromIndex(fromIndex)
            .withTopicCount(topicCount);

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String deleteTopicsWithPrefixAndCount(String topicPrefix, int topicCount) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDeleteSubcommand()
            .withTopicPrefix(topicPrefix)
            .withTopicCount(topicCount);

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String deleteTopicsWithPrefix(String topicPrefix) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDeleteSubcommand()
            .withTopicPrefix(topicPrefix)
            .withAll();

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();
    }

    public KafkaTopicDescription describeTopic(String topicName) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withDescribeSubcommand()
            .withTopicName(topicName)
            .withOutputJson();

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
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

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();

    }

    public String createTopics(String topicPrefix, int topicsCount, int topicPartitions, int topicReplicas) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withCreateSubcommand()
            .withTopicCount(topicsCount)
            .withTopicPartitions(topicPartitions)
            .withTopicReplicas(topicReplicas)
            .withTopicPrefix(topicPrefix);

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();
    }

    public void configureFromEnv() {
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, CMD, "configure", "common", "--from-env");
    }

    public String fetchOffsets(String topicName, String time) {
        AdminTopicCommand adminTopicCommand = new AdminTopicCommand()
            .withFetchOffsetsSubCommand()
            .withTopicName(topicName)
            .withTime(time)
            .withOutputJson();

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminTopicCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();
    }

    public String describeNodes(String nodeIds) {
        AdminNodeCommand adminNodeCommand = new AdminNodeCommand()
            .withDescribeSubcommand()
            .withNodeIds(nodeIds)
            .withOutputJson();

        ExecResult result = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(false, podName, adminNodeCommand.getCommand());
        return result.returnCode() == 0 ? result.out() : result.err();
    }

    static class AdminTopicCommand extends AdminCommonCommand<AdminTopicCommand> {
        public AdminTopicCommand withCreateSubcommand() {
            add("create");
            return this;
        }

        public AdminTopicCommand withDeleteSubcommand() {
            add("delete");
            return this;
        }

        public AdminTopicCommand withAlterSubcommand() {
            add("alter");
            return this;
        }

        public AdminTopicCommand withListSubCommand() {
            add("list");
            return this;
        }

        public AdminTopicCommand withFetchOffsetsSubCommand() {
            add("fetch-offsets");
            return this;
        }

        public AdminTopicCommand withTopicPrefix(String topicPrefix) {
            add(List.of("-tpref", topicPrefix));
            return this;
        }

        public AdminTopicCommand withTopicName(String topicName) {
            add(List.of("-t", topicName));
            return this;
        }

        public AdminTopicCommand withTopicPartitions(int topicPartitions) {
            add(List.of("-tp", String.valueOf(topicPartitions)));
            return this;
        }

        public AdminTopicCommand withTopicCount(int topicCount) {
            add(List.of("-tc", String.valueOf(topicCount)));
            return this;
        }

        public AdminTopicCommand withTopicReplicas(int topicReplicas) {
            add(List.of("-trf", String.valueOf(topicReplicas)));
            return this;
        }

        public AdminTopicCommand withFromIndex(int fromIndex) {
            add(List.of("-fi", String.valueOf(fromIndex)));
            return this;
        }

        public AdminTopicCommand withAll() {
            add("--all");
            return this;
        }

        public AdminTopicCommand withTime(String time) {
            add(List.of("--time", time));
            return this;
        }

        @Override
        protected AdminTopicCommand self() {
            return this;
        }

        @Override
        protected String mainSubCommand() {
            return "topic";
        }
    }

    static class AdminNodeCommand extends AdminCommonCommand<AdminNodeCommand> {
        public AdminNodeCommand withNodeIds(String nodeIds) {
            add(List.of("--node-ids", nodeIds));
            return this;
        }

        @Override
        protected AdminNodeCommand self() {
            return this;
        }

        @Override
        protected String mainSubCommand() {
            return "node";
        }
    }

    static abstract class AdminCommonCommand<T extends AdminCommonCommand<T>> {
        private List<String> command = new ArrayList<>(List.of(CMD, mainSubCommand()));

        protected T add(String toBeAdded) {
            command.add(toBeAdded);
            return self();
        }

        protected T add(List<String> toBeAdded) {
            command.addAll(toBeAdded);
            return self();
        }

        public T withDescribeSubcommand() {
            command.add("describe");
            return self();
        }

        public T withOutputJson() {
            command.add("--output=json");
            return self();
        }

        public String[] getCommand() {
            return command.toArray(new String[0]);
        }

        protected abstract T self();

        protected abstract String mainSubCommand();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.dump;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.USER_PATH;
import static org.hamcrest.MatcherAssert.assertThat;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.test.annotations.IsolatedSuite;
import io.strimzi.test.annotations.IsolatedTest;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@IsolatedSuite
public class LogDumpScriptIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(LogDumpScriptIsolatedST.class);

    @IsolatedTest
    void dumpPartitions(ExtensionContext context) {
        String clusterName = mapWithClusterNames.get(context.getDisplayName());
        String groupId = "my-group";
        String partitionNumber = "0";
        String outPath = USER_PATH + "/target/" + clusterName;
        
        resourceManager.createResource(context, KafkaTemplates.kafkaPersistent(clusterName, 1, 1)
            .editMetadata()
                .withNamespace(INFRA_NAMESPACE)
            .endMetadata()
            .build());
        String clientsPodName = deployAndGetInternalClientsPodName(context);
        InternalKafkaClient clients = buildInternalClients(context, clientsPodName, groupId, 10);
        String topicName = mapWithTestTopics.get(context.getDisplayName());
        
        // send messages and consume them
        clients.sendMessagesPlain();
        clients.receiveMessagesPlain();

        // dry run
        LOGGER.info("Print partition segments from cluster {}/{}", INFRA_NAMESPACE, clusterName);
        String[] printCmd = new String[] {
            USER_PATH + "/../tools/log-dump/run.sh", "partition", "--namespace", INFRA_NAMESPACE, "--cluster", clusterName, "--topic", topicName, "--partition", partitionNumber, "--dry-run"
        };
        Exec.exec(true, printCmd);
        assertThat("Output directory created in dry mode", Files.notExists(Paths.get(outPath)));
        
        // partition dump
        LOGGER.info("Dump topic partition from cluster {}/{}", INFRA_NAMESPACE, clusterName);
        String[] dumpPartCmd = new String[] {
            USER_PATH + "/../tools/log-dump/run.sh", "partition", "--namespace", INFRA_NAMESPACE, "--cluster", clusterName, "--topic", topicName, "--partition", partitionNumber, "--out-path", outPath
        };
        Exec.exec(true, dumpPartCmd);
        assertThat("No output directory created", Files.exists(Paths.get(outPath)));
        String dumpPartFilePath = outPath + "/" + topicName + "/kafka-0-" + topicName + "-" + partitionNumber + "/00000000000000000000.log";
        assertThat("No partition file created", Files.exists(Paths.get(dumpPartFilePath)));
        assertThat("Empty partition file", new File(dumpPartFilePath).length() > 0);
        
        // __consumer_offsets dump
        LOGGER.info("Dump consumer offsets partition from cluster {}/{}", INFRA_NAMESPACE, clusterName);
        String[] dumpCgCmd = new String[] {
            USER_PATH + "/../tools/log-dump/run.sh", "cg_offsets", "--namespace", INFRA_NAMESPACE, "--cluster", clusterName, "--group-id", groupId, "--out-path", outPath
        };
        Exec.exec(true, dumpCgCmd);
        assertThat("No output directory created", Files.exists(Paths.get(outPath)));
        String dumpCgFilePath = outPath + "/__consumer_offsets/kafka-0-__consumer_offsets-12/00000000000000000000.log";
        assertThat("No partition file created", Files.exists(Paths.get(dumpCgFilePath)));
        assertThat("Empty partition file", new File(dumpCgFilePath).length() > 0);
    }

    private String deployAndGetInternalClientsPodName(ExtensionContext context) {
        final String kafkaClientsName = mapWithKafkaClientNames.get(context.getDisplayName());
        resourceManager.createResource(context, KafkaClientsTemplates.kafkaClients(INFRA_NAMESPACE, false, kafkaClientsName).build());
        return ResourceManager.kubeClient().listPodsByPrefixInName(INFRA_NAMESPACE, kafkaClientsName).get(0).getMetadata().getName();
    }

    private InternalKafkaClient buildInternalClients(ExtensionContext context, String podName, String groupId, int batchSize) {
        String clusterName = mapWithClusterNames.get(context.getDisplayName());
        String topicName = mapWithTestTopics.get(context.getDisplayName());
        InternalKafkaClient clients = new InternalKafkaClient.Builder()
                .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                .withNamespaceName(INFRA_NAMESPACE)
                .withUsingPodName(podName)
                .withClusterName(clusterName)
                .withTopicName(topicName)
                .withConsumerGroupName(groupId)
                .withMessageCount(batchSize)
                .build();
        return clients;
    }
}

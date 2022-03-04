/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.backup;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.USER_PATH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.util.Map;

import io.strimzi.systemtest.annotations.StatefulSetTest;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;

import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.test.annotations.IsolatedSuite;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.clients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.executor.Exec;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@StatefulSetTest
@IsolatedSuite
public class ColdBackupScriptIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ColdBackupScriptIsolatedST.class);

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation().createInstallation().runInstallation();
    }

    @IsolatedTest
    void backupAndRestore(ExtensionContext context) {
        String clusterName = mapWithClusterNames.get(context.getDisplayName());
        String groupId = "my-group", newGroupId = "new-group";
        int firstBatchSize = 100, secondBatchSize = 10;
        String backupFilePath = USER_PATH + "/target/" + clusterName + ".tgz";

        resourceManager.createResource(context, KafkaTemplates.kafkaPersistent(clusterName, 1, 1)
                .editMetadata()
                    .withNamespace(INFRA_NAMESPACE)
                .endMetadata()
                .build());
        String clientsPodName = deployAndGetInternalClientsPodName(context);
        InternalKafkaClient clients = buildInternalClients(context, clientsPodName, groupId, firstBatchSize);

        // send messages and consume them
        clients.sendMessagesPlain();
        clients.receiveMessagesPlain();

        // save consumer group offsets
        Map<String, String> offsetsBeforeBackup = clients.getCurrentOffsets();
        assertThat("No offsets map before backup", offsetsBeforeBackup != null && offsetsBeforeBackup.size() > 0);

        // send additional messages
        clients.setMessageCount(secondBatchSize);
        clients.sendMessagesPlain();

        // backup command
        LOGGER.info("Running backup procedure for {}/{}", INFRA_NAMESPACE, clusterName);
        String[] backupCommand = new String[] {
            USER_PATH + "/../tools/cold-backup/run.sh", "backup", "-n", INFRA_NAMESPACE, "-c", clusterName, "-t", backupFilePath, "-y"
        };
        Exec.exec(Level.INFO, backupCommand);

        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation().createInstallation().runInstallation();

        // restore command
        LOGGER.info("Running restore procedure for {}/{}", INFRA_NAMESPACE, clusterName);
        String[] restoreCommand = new String[] {
            USER_PATH + "/../tools/cold-backup/run.sh", "restore", "-n", INFRA_NAMESPACE, "-c", clusterName, "-s", backupFilePath, "-y"
        };
        Exec.exec(Level.INFO, restoreCommand);

        // check consumer group offsets
        KafkaUtils.waitForKafkaReady(clusterName);
        clientsPodName = deployAndGetInternalClientsPodName(context);
        clients = buildInternalClients(context, clientsPodName, groupId, secondBatchSize);
        Map<String, String> offsetsAfterRestore = clients.getCurrentOffsets();
        assertThat("Current consumer group offsets are not the same as before the backup", offsetsAfterRestore, is(offsetsBeforeBackup));

        // check consumer group recovery
        assertThat("Consumer group is not able to recover after restore", clients.receiveMessagesPlain(), is(secondBatchSize));

        // check total number of messages
        int batchSize = firstBatchSize + secondBatchSize;
        clients = clients.toBuilder()
                .withConsumerGroupName(newGroupId)
                .withMessageCount(batchSize)
                .build();
        assertThat("A new consumer group is not able to get all messages", clients.receiveMessagesPlain(), is(batchSize));
    }

    private String deployAndGetInternalClientsPodName(ExtensionContext context) {
        final String kafkaClientsName = mapWithKafkaClientNames.get(context.getDisplayName());
        resourceManager.createResource(context, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName)
            .editMetadata()
                .withNamespace(INFRA_NAMESPACE)
            .endMetadata()
            .build());
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

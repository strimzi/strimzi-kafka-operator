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

import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.test.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.executor.Exec;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@IsolatedSuite
public class ColdBackupScriptST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ColdBackupScriptST.class);

    @Test
    void backupAndRestore(ExtensionContext context) {
        String clusterName = mapWithClusterNames.get(context.getDisplayName());
        String groupName = "my-group", newGroupName = "new-group";
        int firstBatchSize = 100, secondBatchSize = 10;
        String backupFilePath = USER_PATH + "/target/" + clusterName + ".zip";

        resourceManager.createResource(context, KafkaTemplates.kafkaPersistent(clusterName, 1, 1).build());
        String clientsPodName = deployAndGetInternalClientsPodName(context);
        InternalKafkaClient clients = buildInternalClients(context, clientsPodName, groupName, firstBatchSize);

        // send messages and consume them
        clients.sendMessagesPlain();
        clients.receiveMessagesPlain();

        // save consumer group offsets
        Map<String, String> offsetsBeforeBackup = clients.getCurrentOffsets();

        // send additional messages
        clients.setMessageCount(secondBatchSize);
        clients.sendMessagesPlain();

        // run backup procedure
        LOGGER.info("Running backup procedure for {}/{}", INFRA_NAMESPACE, clusterName);
        String[] backupCommand = new String[] {
            USER_PATH + "/../tools/cold-backup/run.sh", "backup", "-n", INFRA_NAMESPACE, "-c", clusterName, "-t", backupFilePath, "-y"
        };
        Exec.exec(true, backupCommand);

        install.unInstall();
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(context)
            .withNamespace(INFRA_NAMESPACE)
            .createInstallation()
            .runInstallation();

        // run restore procedure and wait for provisioning
        LOGGER.info("Running restore procedure for {}/{}", INFRA_NAMESPACE, clusterName);
        String[] restoreCommand = new String[] {
            USER_PATH + "/../tools/cold-backup/run.sh", "restore", "-n", INFRA_NAMESPACE, "-c", clusterName, "-s", backupFilePath, "-y"
        };
        Exec.exec(true, restoreCommand);

        // check consumer group offsets
        KafkaUtils.waitForKafkaReady(clusterName);
        clientsPodName = deployAndGetInternalClientsPodName(context);
        clients = buildInternalClients(context, clientsPodName, groupName, secondBatchSize);
        Map<String, String> offsetsAfterRestore = clients.getCurrentOffsets();
        assertThat("Current consumer group offsets are not the same as before the backup", offsetsAfterRestore, is(offsetsBeforeBackup));

        // check consumer group recovery
        assertThat("Consumer group is not able to recover after restore", clients.receiveMessagesPlain(), is(secondBatchSize));

        // check total number of messages
        int batchSize = firstBatchSize + secondBatchSize;
        clients = clients.toBuilder()
            .withConsumerGroupName(newGroupName)
            .withMessageCount(batchSize)
            .build();
        assertThat("A new consumer group is not able to get all messages", clients.receiveMessagesPlain(), is(batchSize));
    }

    private String deployAndGetInternalClientsPodName(ExtensionContext context) {
        final String kafkaClientsName = mapWithKafkaClientNames.get(context.getDisplayName());
        resourceManager.createResource(context, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        return ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();
    }

    private InternalKafkaClient buildInternalClients(ExtensionContext context, String podName, String groupName, int batchSize) {
        String clusterName = mapWithClusterNames.get(context.getDisplayName());
        String topicName = mapWithTestTopics.get(context.getDisplayName());
        InternalKafkaClient clients = new InternalKafkaClient.Builder()
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .withNamespaceName(INFRA_NAMESPACE)
            .withUsingPodName(podName)
            .withClusterName(clusterName)
            .withTopicName(topicName)
            .withConsumerGroupName(groupName)
            .withMessageCount(batchSize)
            .build();
        return clients;
    }

}

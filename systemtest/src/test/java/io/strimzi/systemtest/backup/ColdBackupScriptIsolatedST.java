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

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;

import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.test.annotations.IsolatedSuite;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.executor.Exec;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@IsolatedSuite
public class ColdBackupScriptIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ColdBackupScriptIsolatedST.class);

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }

    @IsolatedTest
    void backupAndRestore(ExtensionContext context) {
        String clusterName = mapWithClusterNames.get(context.getDisplayName());
        String groupId = "my-group", newGroupId = "new-group";

        String topicName = mapWithTestTopics.get(context.getDisplayName());
        String producerName = clusterName + "-producer";
        String consumerName = clusterName + "-consumer";

        int firstBatchSize = 100, secondBatchSize = 10;
        String backupFilePath = USER_PATH + "/target/" + clusterName + ".tgz";

        resourceManager.createResource(context, KafkaTemplates.kafkaPersistent(clusterName, 1, 1)
            .editMetadata()
                .withNamespace(INFRA_NAMESPACE)
            .endMetadata()
            .build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withNamespaceName(INFRA_NAMESPACE)
            .withTopicName(topicName)
            .withConsumerGroup(groupId)
            .withMessageCount(firstBatchSize)
            .build();

        // send messages and consume them
        resourceManager.createResource(context, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(producerName, consumerName, INFRA_NAMESPACE, firstBatchSize);

        // save consumer group offsets
        int offsetsBeforeBackup = KafkaUtils.getCurrentOffsets(KafkaResources.kafkaPodName(clusterName, 0), topicName, groupId);
        assertThat("No offsets map before backup", offsetsBeforeBackup > 0);

        // send additional messages
        clients = new KafkaClientsBuilder(clients)
            .withMessageCount(secondBatchSize)
            .build();

        resourceManager.createResource(context, clients.producerStrimzi());
        ClientUtils.waitForClientSuccess(producerName, INFRA_NAMESPACE, firstBatchSize);

        // backup command
        LOGGER.info("Running backup procedure for {}/{}", INFRA_NAMESPACE, clusterName);
        String[] backupCommand = new String[] {
            USER_PATH + "/../tools/cold-backup/run.sh", "backup", "-n", INFRA_NAMESPACE, "-c", clusterName, "-t", backupFilePath, "-y"
        };
        Exec.exec(Level.INFO, backupCommand);

        clusterOperator.unInstall();

        clusterOperator = clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        // restore command
        LOGGER.info("Running restore procedure for {}/{}", INFRA_NAMESPACE, clusterName);
        String[] restoreCommand = new String[] {
            USER_PATH + "/../tools/cold-backup/run.sh", "restore", "-n", INFRA_NAMESPACE, "-c", clusterName, "-s", backupFilePath, "-y"
        };
        Exec.exec(Level.INFO, restoreCommand);

        // check consumer group offsets
        KafkaUtils.waitForKafkaReady(clusterName);

        clients = new KafkaClientsBuilder(clients)
            .withMessageCount(secondBatchSize)
            .build();

        int offsetsAfterRestore = KafkaUtils.getCurrentOffsets(KafkaResources.kafkaPodName(clusterName, 0), topicName, groupId);
        assertThat("Current consumer group offsets are not the same as before the backup", offsetsAfterRestore, is(offsetsBeforeBackup));

        // check consumer group recovery
        resourceManager.createResource(context, clients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(consumerName, INFRA_NAMESPACE, secondBatchSize);
        JobUtils.deleteJobWithWait(INFRA_NAMESPACE, consumerName);

        // check total number of messages
        int batchSize = firstBatchSize + secondBatchSize;
        clients = new KafkaClientsBuilder(clients)
                .withConsumerGroup(newGroupId)
                .withMessageCount(batchSize)
                .build();

        resourceManager.createResource(context, clients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(consumerName, INFRA_NAMESPACE, batchSize);
        JobUtils.deleteJobWithWait(INFRA_NAMESPACE, consumerName);
    }
}

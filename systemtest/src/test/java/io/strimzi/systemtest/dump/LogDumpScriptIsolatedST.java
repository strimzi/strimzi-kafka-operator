/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.dump;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.USER_PATH;
import static org.hamcrest.MatcherAssert.assertThat;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.test.annotations.IsolatedTest;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeAll;
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

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }

    @IsolatedTest
    void dumpPartitions(ExtensionContext context) {
        TestStorage testStorage = new TestStorage(context);

        String groupId = "my-group";
        String partitionNumber = "0";
        String outPath = USER_PATH + "/target/" + testStorage.getClusterName();
        
        resourceManager.createResource(context, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(10)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withConsumerGroup(groupId)
            .build();

        // send messages and consume them
        resourceManager.createResource(context, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        // dry run
        LOGGER.info("Print partition segments from cluster {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        String[] printCmd = new String[] {
            USER_PATH + "/../tools/log-dump/run.sh", "partition", "--namespace", testStorage.getNamespaceName(), "--cluster",
            testStorage.getClusterName(), "--topic", testStorage.getTopicName(), "--partition", partitionNumber, "--dry-run"
        };
        Exec.exec(Level.INFO, printCmd);
        assertThat("Output directory created in dry mode", Files.notExists(Paths.get(outPath)));
        
        // partition dump
        LOGGER.info("Dump topic partition from cluster {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        String[] dumpPartCmd = new String[] {
            USER_PATH + "/../tools/log-dump/run.sh", "partition", "--namespace", testStorage.getNamespaceName(), "--cluster",
            testStorage.getClusterName(), "--topic", testStorage.getTopicName(), "--partition", partitionNumber, "--out-path", outPath
        };
        Exec.exec(Level.INFO, dumpPartCmd);
        assertThat("No output directory created", Files.exists(Paths.get(outPath)));
        String dumpPartFilePath = outPath + "/" + testStorage.getTopicName() + "/kafka-0-" + testStorage.getTopicName() + "-" + partitionNumber + "/00000000000000000000.log";
        assertThat("No partition file created", Files.exists(Paths.get(dumpPartFilePath)));
        assertThat("Empty partition file", new File(dumpPartFilePath).length() > 0);
        
        // __consumer_offsets dump
        LOGGER.info("Dump consumer offsets partition from cluster {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        String[] dumpCgCmd = new String[] {
            USER_PATH + "/../tools/log-dump/run.sh", "cg_offsets", "--namespace", testStorage.getNamespaceName(), "--cluster",
            testStorage.getClusterName(), "--group-id", groupId, "--out-path", outPath
        };
        Exec.exec(Level.INFO, dumpCgCmd);
        assertThat("No output directory created", Files.exists(Paths.get(outPath)));
        String dumpCgFilePath = outPath + "/__consumer_offsets/kafka-0-__consumer_offsets-12/00000000000000000000.log";
        assertThat("No partition file created", Files.exists(Paths.get(dumpCgFilePath)));
        assertThat("Empty partition file", new File(dumpCgFilePath).length() > 0);
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import kafka.admin.ReassignPartitionsCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TopicOperatorReplicationIT extends TopicOperatorBaseIT {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorReplicationIT.class);

    @Override
    protected int numKafkaBrokers() {
        return 2;
    }

    @Override
    protected Properties kafkaClusterConfig() {
        return new Properties();
    }

    @Override
    protected Map<String, String> topicOperatorConfig() {
        Map<String, String> m = super.topicOperatorConfig();
        m.put(Config.FULL_RECONCILIATION_INTERVAL_MS.key, "20000");
        return m;
    }

    @Test
    public void testKafkaTopicModifiedChangedReplication() throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-with-changed-replication";
        String resourceName = createTopic(topicName, asList(1));

        // now change the topicResource
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(resourceName).get())
                .editOrNewSpec().withReplicas(2).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(resourceName).patch(changedTopic);
        assertStatusNotReady(topicName,
                "Changing 'spec.replicas' is not supported. " +
                        "This KafkaTopic's 'spec.replicas' should be reverted to 1 and then " +
                        "the replication should be changed directly in Kafka.");

        // Now do the revert
        changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(resourceName).get())
                .editOrNewSpec().withReplicas(1).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(resourceName).patch(changedTopic);
        assertStatusReady(topicName);

        File file = File.createTempFile(getClass().getSimpleName(), ".json");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = new ObjectNode(mapper.getNodeFactory());
        root.put("version", 1)
            .putArray("partitions")
                .addObject()
                    .put("topic", topicName)
                    .put("partition", 0)
                    .putArray("replicas")
                        .add(1)
                        .add(2);
        mapper.writeValue(file, root);
        LOGGER.info("Creating 2nd replica: {}", mapper.writeValueAsString(root));

        // Now change it in Kafka
        doReassignmentCommand(
                "--bootstrap-server", kafkaCluster.brokerList(),
                "--reassignment-json-file", file.getAbsolutePath(),
                "--execute");

        LOGGER.info("Waiting for reassignment completion");
        waitFor(() -> {
            String output = doReassignmentCommand(
                    "--bootstrap-server", kafkaCluster.brokerList(),
                    "--reassignment-json-file", file.getAbsolutePath(),
                    "--verify");
            LOGGER.info(output);

            if (output.contains("Reassignment of partition test-kafkatopic-modified-with-changed-replication-0 is still in progress")) {
                return false;
            } else {
                assertThat("Reassignment is no longer in progress, but wasn't successful: " + output,
                        output.contains("Reassignment of partition test-kafkatopic-modified-with-changed-replication-0 is complete"), is(true));
                return true;
            }
        }, "reassignment completion");

        // wait for reconciliation and that now replicas=2.
        waitFor(() -> {
            KafkaTopic kafkaTopic = Crds.topicOperation(kubeClient).inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info(kafkaTopic == null ? "Null topic" : kafkaTopic.toString());
            return kafkaTopic.getSpec().getReplicas() == 2;
        }, "KafkaTopic.spec.replicas=2");

        // And check that the status is ready
        assertStatusReady(topicName);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream tempStdOut = new PrintStream(baos);

    private String doReassignmentCommand(String... args) {
        PrintStream originalStdOut = System.out;
        PrintStream originalStdErr = System.err;
        System.setOut(tempStdOut);
        System.setErr(tempStdOut);
        ReassignPartitionsCommand.main(args);
        System.setOut(originalStdOut);
        System.setErr(originalStdErr);
        tempStdOut.flush();
        String s = new String(baos.toByteArray());
        baos.reset();
        return s;
    }

}


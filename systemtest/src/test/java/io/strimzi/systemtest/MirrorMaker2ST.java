/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;

@Tag(REGRESSION)
class MirrorMaker2ST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2ST.class);
    public static final String NAMESPACE = "mirrormaker2-cluster-test";

    private static final String MIRRORMAKER2_TOPIC_NAME = "mirrormaker2-topic-example";
    private final int messagesCount = 200;

    private String kafkaClusterSourceName = CLUSTER_NAME + "-source";
    private String kafkaClusterTargetName = CLUSTER_NAME + "-target";

    @Test
    void testMirrorMaker2() throws Exception {
        Map<String, Object> expectedConfig = StUtils.loadProperties("group.id=mirrormaker2-cluster\n" +
                "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "config.storage.topic=mirrormaker2-cluster-configs\n" +
                "status.storage.topic=mirrormaker2-cluster-status\n" +
                "offset.storage.topic=mirrormaker2-cluster-offsets\n" +
                "config.storage.replication.factor=1\n" +
                "status.storage.replication.factor=1\n" +
                "offset.storage.replication.factor=1\n");

        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        // Deploy source kafka
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).done();
        // Deploy target kafka
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).done();
        // Deploy Topic
        KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName).done();

        KafkaClientsResource.deployKafkaClients(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        // Check brokers availability
        availabilityTest(messagesCount, kafkaClusterSourceName);
        availabilityTest(messagesCount, kafkaClusterTargetName);
        
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1).done();
        LOGGER.info("Looks like the mirrormaker2 cluster my-cluster deployed OK");

        String podName = PodUtils.getPodNameByPrefix(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(expectedConfig));
        testDockerImagesForKafkaMirrorMaker2();

        verifyLabelsOnPods(CLUSTER_NAME, "mirrormaker2", null, "KafkaMirrorMaker2");
        verifyLabelsForService(CLUSTER_NAME, "mirrormaker2-api", "KafkaMirrorMaker2");

        verifyLabelsForConfigMaps(kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(kafkaClusterSourceName, null);

        final String kafkaClientsPodName = PodUtils.getPodNameByPrefix(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS);

        int sent = sendMessages(messagesCount,  kafkaClusterSourceName, false, topicSourceName, null, kafkaClientsPodName);
        int receivedSource = receiveMessages(messagesCount, kafkaClusterSourceName, false, topicSourceName, null, kafkaClientsPodName);
        int receivedTarget = receiveMessages(messagesCount, kafkaClusterTargetName, false, topicTargetName, null, kafkaClientsPodName);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    private void testDockerImagesForKafkaMirrorMaker2() {
        LOGGER.info("Verifying docker image names");
        Map<String, String> imgFromDeplConf = getImagesFromConfig();
        //Verifying docker image for kafka mirrormaker2
        String mirrormaker2ImageName = PodUtils.getFirstContainerImageNameFromPod(kubeClient().listPods("strimzi.io/kind", "KafkaMirrorMaker2").
                get(0).getMetadata().getName());

        String mirrormaker2Version = Crds.kafkaMirrorMaker2Operation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getVersion();
        if (mirrormaker2Version == null) {
            mirrormaker2Version = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_MIRROR_MAKER_2_IMAGE_MAP)).get(mirrormaker2Version), is(mirrormaker2ImageName));
        LOGGER.info("Docker images verified");
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        cluster.setNamespace(NAMESPACE);
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }
}
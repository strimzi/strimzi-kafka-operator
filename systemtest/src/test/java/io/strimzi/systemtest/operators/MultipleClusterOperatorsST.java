/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.hamcrest.CoreMatchers.is;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
public class MultipleClusterOperatorsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleClusterOperatorsST.class);

    public static final String DEFAULT_NAMESPACE = "multiple-co-cluster-test";
    public static final String FIRST_NAMESPACE = "first-co-namespace";
    public static final String SECOND_NAMESPACE = "second-co-namespace";

    public static final String FIRST_CO_NAME = "first-" + Constants.STRIMZI_DEPLOYMENT_NAME;
    public static final String SECOND_CO_NAME = "second-" + Constants.STRIMZI_DEPLOYMENT_NAME;

    public static final EnvVar FIRST_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "co-name=" + FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "co-name=" + SECOND_CO_NAME, null);

    public static final Map<String, String> FIRST_CO_SELECTOR = Collections.singletonMap("co-name", FIRST_CO_NAME);
    public static final Map<String, String> SECOND_CO_SELECTOR = Collections.singletonMap("co-name", SECOND_CO_NAME);

    @Test
    void testMultipleCOsWithDifferentCustomResourceSelector() {
        deployCOInNamespace(FIRST_CO_NAME, DEFAULT_NAMESPACE, FIRST_CO_SELECTOR_ENV, false);
        deployCOInNamespace(SECOND_CO_NAME, DEFAULT_NAMESPACE, SECOND_CO_SELECTOR_ENV, false);

        testMultipleCOsWithDifferentCRSelectors();
    }

    @Test
    void testMultipleCOsInDifferentNamespaces() {
        deployCOInNamespace(FIRST_CO_NAME, FIRST_NAMESPACE, FIRST_CO_SELECTOR_ENV, true);
        deployCOInNamespace(SECOND_CO_NAME, SECOND_NAMESPACE, SECOND_CO_SELECTOR_ENV, true);

        cluster.setNamespace(DEFAULT_NAMESPACE);

        testMultipleCOsWithDifferentCRSelectors();
    }

    void testMultipleCOsWithDifferentCRSelectors() {
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";

        LOGGER.info("Deploying Kafka with CR selector pointing at first CO");
        KafkaResource.kafkaWithoutWait(KafkaResource.kafkaEphemeral(clusterName, 3, 3).build());

        PodUtils.waitUntilPodStabilityReplicasCount(clusterName, 0);

        LOGGER.info("Add selector for {} into Kafka CR", FIRST_CO_NAME);
        KafkaResource.replaceKafkaResource(clusterName, kafka -> kafka.getMetadata().setLabels(FIRST_CO_SELECTOR));
        KafkaUtils.waitForKafkaReady(clusterName);

        LOGGER.info("Deploying topic with different CR selector than Kafka has");
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME)
            .editOrNewMetadata()
                .addToLabels(SECOND_CO_SELECTOR)
            .endMetadata()
            .build());

        KafkaConnectResource.createAndWaitForReadiness(KafkaConnectResource.kafkaConnect(clusterName, 1)
            .editOrNewMetadata()
                .addToLabels(FIRST_CO_SELECTOR)
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build(), false);

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaConnector with file sink and different CR selector than Kafka");
        KafkaConnectorResource.createAndWaitForReadiness(KafkaConnectorResource.kafkaConnector(clusterName)
            .editOrNewMetadata()
                .addToLabels(SECOND_CO_SELECTOR)
            .endMetadata()
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", TOPIC_NAME)
            .endSpec()
            .build());

        KafkaBasicExampleClients basicClients = new KafkaBasicExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        basicClients.createAndWaitForReadiness(basicClients.producerStrimzi().build());
        ClientUtils.waitForClientSuccess(producerName, DEFAULT_NAMESPACE, MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world - 99");
    }

    @Test
    void testLabelingAndUnlabelingOfResource() {
        int scaleTo = 4;
        deployCOInNamespace(FIRST_CO_NAME, DEFAULT_NAMESPACE, FIRST_CO_SELECTOR_ENV, false);
        deployCOInNamespace(SECOND_CO_NAME, DEFAULT_NAMESPACE, SECOND_CO_SELECTOR_ENV, false);

        LOGGER.info("Deploying Kafka with CR selector of first CO");
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3)
            .editOrNewMetadata()
                .addToLabels(FIRST_CO_SELECTOR)
            .endMetadata()
            .build());

        LOGGER.info("Removing CR selector from Kafka and increasing number of replicas to 4, new pod should not appear");
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getMetadata().getLabels().clear();
            kafka.getSpec().getKafka().setReplicas(scaleTo);
        });
        KafkaUtils.waitForClusterStability(clusterName);

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        LOGGER.info("Adding CR selector of second CO to Kafka");
        KafkaResource.replaceKafkaResource(clusterName, kafka -> kafka.getMetadata().setLabels(SECOND_CO_SELECTOR));

        LOGGER.info("Waiting for rolling update because of new label, also Kafka should scale to {}", scaleTo);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), scaleTo, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        Kafka kafka = KafkaResource.kafkaClient().inNamespace(DEFAULT_NAMESPACE).withName(clusterName).get();
        assertThat(kafka.getSpec().getKafka().getReplicas(), is(scaleTo));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName)).size(), is(scaleTo));
    }

    @Test
    void testKafkaCCAndRebalanceWithDifferentCRSelectors() {
        deployCOInNamespace(FIRST_CO_NAME, DEFAULT_NAMESPACE, FIRST_CO_SELECTOR_ENV, false);
        deployCOInNamespace(SECOND_CO_NAME, DEFAULT_NAMESPACE, SECOND_CO_SELECTOR_ENV, false);

        LOGGER.info("Deploying Kafka with CruiseControl and CR selector of first CO");
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaWithCruiseControl(clusterName, 3, 3)
            .editOrNewMetadata()
                .addToLabels(FIRST_CO_SELECTOR)
            .endMetadata()
            .build());

        LOGGER.info("Creating KafkaRebalance with different CR selector than CC");
        KafkaRebalanceResource.createAndWaitForReadiness(KafkaRebalanceResource.kafkaRebalance(clusterName)
            .editOrNewMetadata()
                .addToLabels(SECOND_CO_SELECTOR)
            .endMetadata()
            .build());

        KafkaRebalanceUtils.doRebalancingProcess(clusterName);
    }

    void deployCOInNamespace(String coName, String coNamespace, EnvVar selectorEnv, boolean multipleNamespaces) {
        String namespace = multipleNamespaces ? "*" : coNamespace;

        if (multipleNamespaces) {
            prepareEnvForOperator(coNamespace);

            // Apply rolebindings in CO namespace
            applyBindings(coNamespace);

            // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
            List<ClusterRoleBinding> clusterRoleBindingList = KubernetesResource.clusterRoleBindingsForAllNamespaces(coNamespace, coName);
            clusterRoleBindingList.forEach(KubernetesResource::clusterRoleBinding);
        }


        LOGGER.info("Creating {} in {} namespace", coName, coNamespace);

        BundleResource.createAndWaitForReadiness(BundleResource.clusterOperator(namespace)
            .editOrNewMetadata()
                .withName(coName)
            .endMetadata()
            .editOrNewSpec()
                .editOrNewSelector()
                    .addToMatchLabels("co-name", coName)
                .endSelector()
                .editOrNewTemplate()
                    .editOrNewMetadata()
                        .addToLabels("co-name", coName)
                    .endMetadata()
                    .editOrNewSpec()
                        .editContainer(0)
                            .addToEnv(selectorEnv)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }

    @BeforeAll
    void setup() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
        ResourceManager.setClassResources();
        prepareEnvForOperator(DEFAULT_NAMESPACE);
        applyBindings(DEFAULT_NAMESPACE);
    }
}

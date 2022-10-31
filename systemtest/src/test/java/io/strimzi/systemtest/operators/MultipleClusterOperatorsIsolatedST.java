/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.hamcrest.CoreMatchers.is;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
@IsolatedSuite
public class MultipleClusterOperatorsIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleClusterOperatorsIsolatedST.class);

    public static final String DEFAULT_NAMESPACE = "multiple-co-cluster-test";
    public static final String FIRST_NAMESPACE = "first-co-namespace";
    public static final String SECOND_NAMESPACE = "second-co-namespace";

    public static final String FIRST_CO_NAME = "first-" + Constants.STRIMZI_DEPLOYMENT_NAME;
    public static final String SECOND_CO_NAME = "second-" + Constants.STRIMZI_DEPLOYMENT_NAME;

    public static final EnvVar FIRST_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "app.kubernetes.io/operator=" + FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "app.kubernetes.io/operator=" + SECOND_CO_NAME, null);

    public static final EnvVar FIRST_CO_LEASE_NAME_ENV = new EnvVar("STRIMZI_LEADER_ELECTION_LEASE_NAME", FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_LEASE_NAME_ENV = new EnvVar("STRIMZI_LEADER_ELECTION_LEASE_NAME", SECOND_CO_NAME, null);

    public static final Map<String, String> FIRST_CO_SELECTOR = Collections.singletonMap("app.kubernetes.io/operator", FIRST_CO_NAME);
    public static final Map<String, String> SECOND_CO_SELECTOR = Collections.singletonMap("app.kubernetes.io/operator", SECOND_CO_NAME);

    @IsolatedTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testMultipleCOsInDifferentNamespaces(ExtensionContext extensionContext) {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";

        deployCOInNamespace(extensionContext, FIRST_CO_NAME, FIRST_NAMESPACE, Collections.singletonList(FIRST_CO_SELECTOR_ENV), true);
        deployCOInNamespace(extensionContext, SECOND_CO_NAME, SECOND_NAMESPACE, Collections.singletonList(SECOND_CO_SELECTOR_ENV), true);

        cluster.createNamespace(DEFAULT_NAMESPACE);
        cluster.setNamespace(DEFAULT_NAMESPACE);

        LOGGER.info("Deploying Kafka without CR selector");
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(DEFAULT_NAMESPACE)
            .endMetadata()
            .build());

        // checking that no pods with prefix 'clusterName' will be created in some time
        PodUtils.waitUntilPodStabilityReplicasCount(DEFAULT_NAMESPACE, clusterName, 0);

        LOGGER.info("Adding {} selector of {} into Kafka CR", FIRST_CO_SELECTOR, FIRST_CO_NAME);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getMetadata().setLabels(FIRST_CO_SELECTOR), DEFAULT_NAMESPACE);
        KafkaUtils.waitForKafkaReady(DEFAULT_NAMESPACE, clusterName);

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(clusterName, topicName)
                .editMetadata()
                    .withNamespace(DEFAULT_NAMESPACE)
                .endMetadata()
                .build(),
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(clusterName, DEFAULT_NAMESPACE, 1)
                .editOrNewMetadata()
                    .addToLabels(FIRST_CO_SELECTOR)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                    .withNamespace(DEFAULT_NAMESPACE)
                .endMetadata()
                .build());

        String kafkaConnectPodName = kubeClient(DEFAULT_NAMESPACE).listPods(DEFAULT_NAMESPACE, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaConnector with file sink and CR selector - {} - different than selector in Kafka", SECOND_CO_SELECTOR);
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editOrNewMetadata()
                .addToLabels(SECOND_CO_SELECTOR)
                .withNamespace(DEFAULT_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        KafkaClients basicClients = new KafkaClientsBuilder()
            .withNamespaceName(DEFAULT_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResource(extensionContext, basicClients.producerStrimzi());
        ClientUtils.waitForClientSuccess(producerName, DEFAULT_NAMESPACE, MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(DEFAULT_NAMESPACE, kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world - 99");
    }

    @IsolatedTest
    @Tag(CRUISE_CONTROL)
    void testKafkaCCAndRebalanceWithMultipleCOs(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        int scaleTo = 4;

        deployCOInNamespace(extensionContext, FIRST_CO_NAME, DEFAULT_NAMESPACE, List.of(FIRST_CO_SELECTOR_ENV, FIRST_CO_LEASE_NAME_ENV), false);
        deployCOInNamespace(extensionContext, SECOND_CO_NAME, DEFAULT_NAMESPACE, List.of(SECOND_CO_SELECTOR_ENV, SECOND_CO_LEASE_NAME_ENV), false);

        LOGGER.info("Deploying Kafka with {} selector of {}", FIRST_CO_SELECTOR, FIRST_CO_NAME);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
            .editOrNewMetadata()
                .addToLabels(FIRST_CO_SELECTOR)
                .withNamespace(DEFAULT_NAMESPACE)
            .endMetadata()
            .build());

        LOGGER.info("Removing CR selector from Kafka and increasing number of replicas to 4, new pod should not appear");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getMetadata().getLabels().clear();
            kafka.getSpec().getKafka().setReplicas(scaleTo);
        }, DEFAULT_NAMESPACE);

        // because KafkaRebalance is pointing to Kafka with CC cluster, we need to create KR before adding the label back
        // to test if KR will be ignored
        LOGGER.info("Creating KafkaRebalance when CC doesn't have label for CO, the KR should be ignored");
        resourceManager.createResource(extensionContext, false, KafkaRebalanceTemplates.kafkaRebalance(clusterName)
            .editMetadata()
                .withNamespace(DEFAULT_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal",
                    "NetworkInboundCapacityGoal", "MinTopicLeadersPerBrokerGoal",
                    "NetworkOutboundCapacityGoal", "ReplicaCapacityGoal")
                .withSkipHardGoalCheck(true)
                // skip sanity check: because of removal 'RackAwareGoal'
            .endSpec()
            .build());

        KafkaUtils.waitForClusterStability(DEFAULT_NAMESPACE, clusterName);

        LOGGER.info("Checking if KafkaRebalance is still ignored, after the cluster stability wait");

        // because KR is ignored, it shouldn't contain any status
        assertNull(KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(DEFAULT_NAMESPACE).withName(clusterName).get().getStatus());

        LOGGER.info("Adding {} selector of {} to Kafka", SECOND_CO_SELECTOR, SECOND_CO_NAME);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getMetadata().setLabels(SECOND_CO_SELECTOR), DEFAULT_NAMESPACE);

        LOGGER.info("Waiting for Kafka to scales pods to {}", scaleTo);
        RollingUpdateUtils.waitForComponentAndPodsReady(DEFAULT_NAMESPACE, kafkaSelector, scaleTo);

        assertThat(PodUtils.podSnapshot(DEFAULT_NAMESPACE, kafkaSelector).size(), is(scaleTo));

        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, SECOND_NAMESPACE, clusterName), DEFAULT_NAMESPACE, clusterName);
    }

    void deployCOInNamespace(ExtensionContext extensionContext, String coName, String coNamespace, List<EnvVar> extraEnvs, boolean multipleNamespaces) {
        String namespace = multipleNamespaces ? Constants.WATCH_ALL_NAMESPACES : coNamespace;

        if (multipleNamespaces) {
            // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
            List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(coNamespace, coName);
            clusterRoleBindingList.forEach(
                clusterRoleBinding -> ResourceManager.getInstance().createResource(extensionContext, clusterRoleBinding));
        }

        LOGGER.info("Creating {} in {} namespace", coName, coNamespace);

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(coNamespace)
            .withClusterOperatorName(coName)
            .withWatchingNamespaces(namespace)
            .withExtraLabels(Collections.singletonMap("app.kubernetes.io/operator", coName))
            .withExtraEnvVars(extraEnvs)
            .createInstallation()
            .runBundleInstallation();
    }

    @BeforeAll
    void setup() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        clusterOperator.unInstall();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.api.kafka.model.user.acl.AclRuleBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaSecretCertificates;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaStatusCertificates;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite containing Custom Resource status verification scenarios, ensuring proper status reporting for Kafka, KafkaConnect, KafkaBridge, KafkaUser, and KafkaMirrorMaker2 resources."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with custom configuration.", expected = "Cluster Operator is deployed with operation timeout settings."),
        @Step(value = "Deploy shared Kafka cluster with multiple listeners.", expected = "Kafka cluster is deployed and ready with plain, TLS, and external listeners.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
class CustomResourceStatusST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(CustomResourceStatusST.class);
    private TestStorage sharedTestStorage;

    @ParallelTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("This test verifies that Kafka cluster status is correctly reported, including observed generation updates and listener status information. The test also verifies recovery from NotReady state."),
        steps = {
            @Step(value = "Verify Kafka cluster is ready and functioning.", expected = "Kafka cluster is ready and can produce/consume messages."),
            @Step(value = "Check initial Kafka status information.", expected = "Kafka status shows correct observed generation and listener details."),
            @Step(value = "Modify Kafka resources to cause NotReady state.", expected = "Kafka cluster becomes NotReady due to insufficient CPU resources."),
            @Step(value = "Restore Kafka resources to recover.", expected = "Kafka cluster returns to Ready state with updated observed generation.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testKafkaStatus() {
        LOGGER.info("Checking status of deployed Kafka cluster");
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withClusterName(sharedTestStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );

        assertKafkaStatus(1, KafkaResources.bootstrapServiceName(sharedTestStorage.getClusterName()) + "." + Environment.TEST_SUITE_NAMESPACE + ".svc");

        ResourceRequirements resources = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("100000m"))
            .build();

        KafkaNodePoolUtils.replace(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setResources(resources));

        LOGGER.info("Waiting for cluster to be in NotReady state");
        KafkaUtils.waitForKafkaNotReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        LOGGER.info("Recover cluster to Ready state");
        resources.setRequests(Collections.singletonMap("cpu", new Quantity("100m")));

        KafkaNodePoolUtils.replace(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setResources(resources));

        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        assertKafkaStatus(3, KafkaResources.bootstrapServiceName(sharedTestStorage.getClusterName()) + "." + Environment.TEST_SUITE_NAMESPACE + ".svc");
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("This test verifies that KafkaUser status is correctly reported during creation, authorization changes, and ready state transitions."),
        steps = {
            @Step(value = "Create KafkaUser with ACL authorization rules.", expected = "KafkaUser is created but not ready due to missing topic."),
            @Step(value = "Remove authorization from KafkaUser specification.", expected = "KafkaUser becomes ready after authorization removal."),
            @Step(value = "Verify KafkaUser status condition.", expected = "KafkaUser status shows Ready condition type.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testKafkaUserStatus() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final AclRule aclRule = new AclRuleBuilder()
            .withNewAclRuleTopicResource()
                .withName(testStorage.getTopicName())
            .endAclRuleTopicResource()
            .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .build();

        KubeResourceManager.get().createResourceWithoutWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), sharedTestStorage.getClusterName())
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .withAcls(aclRule)
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build());

        KafkaUserUtils.waitForKafkaUserNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername());

        KafkaUserUtils.replace(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), user -> user.getSpec().setAuthorization(null));
        KafkaUserUtils.waitForKafkaUserReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername());

        LOGGER.info("Checking status of deployed KafkaUser");
        Condition kafkaCondition = CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(Ready.toString()));
        LOGGER.info("KafkaUser is in desired state: Ready");
    }

    @ParallelTest
    @Tag(BRIDGE)
    @TestDoc(
        description = @Desc("This test verifies that KafkaBridge status is correctly reported, including observed generation updates and URL information during ready and not ready states."),
        steps = {
            @Step(value = "Deploy KafkaBridge and verify initial status.", expected = "KafkaBridge is ready with correct status information including URL."),
            @Step(value = "Modify KafkaBridge resources to cause NotReady state.", expected = "KafkaBridge becomes NotReady due to insufficient CPU resources."),
            @Step(value = "Restore KafkaBridge resources to recover.", expected = "KafkaBridge returns to Ready state with updated observed generation.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.BRIDGE)
        }
    )
    void testKafkaBridgeStatus() {
        String bridgeUrl = KafkaBridgeResources.url(sharedTestStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 8080);

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()), 1).build());
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        assertKafkaBridgeStatus(1, bridgeUrl);

        KafkaBridgeUtils.replace(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaBridgeUtils.waitForKafkaBridgeNotReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        KafkaBridgeUtils.replace(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        assertKafkaBridgeStatus(3, bridgeUrl);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("This test verifies that KafkaConnect and KafkaConnector status are correctly reported during various state transitions including ready, not ready, and configuration changes."),
        steps = {
            @Step(value = "Deploy KafkaConnect with file plugin and KafkaConnector.", expected = "KafkaConnect and KafkaConnector are ready with correct status information."),
            @Step(value = "Modify KafkaConnect bootstrap servers to invalid value.", expected = "KafkaConnect becomes NotReady due to invalid bootstrap configuration."),
            @Step(value = "Restore valid KafkaConnect bootstrap servers.", expected = "KafkaConnect returns to Ready state."),
            @Step(value = "Modify KafkaConnector cluster label to invalid value.", expected = "KafkaConnector becomes NotReady and connector status becomes null."),
            @Step(value = "Restore valid KafkaConnector cluster label.", expected = "KafkaConnector returns to Ready state."),
            @Step(value = "Modify KafkaConnector class name to invalid value.", expected = "KafkaConnector becomes NotReady due to invalid connector class."),
            @Step(value = "Restore valid KafkaConnector configuration.", expected = "KafkaConnector returns to Ready state.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testKafkaConnectAndConnectorStatus() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String connectUrl = KafkaConnectResources.url(sharedTestStorage.getClusterName(), testStorage.getNamespaceName(), 8083);

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), sharedTestStorage.getClusterName())
            .editSpec()
                .addToConfig("topic", testStorage.getTopicName())
            .endSpec()
            .build());

        assertKafkaConnectStatus(1, connectUrl);

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(),
            kb -> kb.getSpec().setBootstrapServers("non-existing-bootstrap"));

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(),
            kb -> kb.getSpec().setBootstrapServers(KafkaResources.tlsBootstrapAddress(sharedTestStorage.getClusterName())));

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        assertKafkaConnectStatus(3, connectUrl);

        KafkaConnectorUtils.replace(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(),
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, "non-existing-connect-cluster")));
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        assertThat(CrdClients.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(sharedTestStorage.getClusterName()).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorUtils.replace(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(),
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, sharedTestStorage.getClusterName())));

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());

        String defaultClass = CrdClients.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(sharedTestStorage.getClusterName()).get().getSpec().getClassName();

        KafkaConnectorUtils.replace(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(),
            kc -> kc.getSpec().setClassName("non-existing-class"));
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        assertThat(CrdClients.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(sharedTestStorage.getClusterName()).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorUtils.replace(testStorage.getNamespaceName(), sharedTestStorage.getClusterName(),
            kc -> {
                kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, sharedTestStorage.getClusterName()));
                kc.getSpec().setClassName(defaultClass);
            });

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
    }

    @ParallelTest
    @Tag(CONNECTOR_OPERATOR)
    @TestDoc(
        description = @Desc("This test verifies that KafkaConnector without proper cluster configuration fails gracefully without causing NullPointerException in Cluster Operator logs."),
        steps = {
            @Step(value = "Create KafkaConnector without cluster configuration in labels.", expected = "KafkaConnector is created but becomes NotReady."),
            @Step(value = "Verify connector remains in NotReady state.", expected = "KafkaConnector status shows NotReady without causing NPE in CO logs."),
            @Step(value = "Delete the invalid KafkaConnector.", expected = "KafkaConnector is successfully deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testKafkaConnectorWithoutClusterConfig() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // This test check NPE when connect cluster is not specified in labels
        // Check for NPE in CO logs is performed after every test in BaseST
        KubeResourceManager.get().createResourceWithoutWait(KafkaConnectorTemplates.kafkaConnector(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), sharedTestStorage.getClusterName(), 2).build());

        KafkaConnectorUtils.waitForConnectorNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());

        CrdClients.kafkaConnectorClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaConnectorUtils.waitForConnectorDeletion(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("This test verifies that certificates reported in Kafka status match the certificates stored in the cluster CA secret."),
        steps = {
            @Step(value = "Retrieve certificates from Kafka status.", expected = "Certificates are successfully retrieved from Kafka status."),
            @Step(value = "Retrieve certificates from cluster CA secret.", expected = "Certificates are successfully retrieved from secret."),
            @Step(value = "Compare status and secret certificates.", expected = "Certificates from status and secret are identical.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testKafkaStatusCertificate() {
        String certs = getKafkaStatusCertificates(Environment.TEST_SUITE_NAMESPACE, TestConstants.TLS_LISTENER_DEFAULT_NAME, sharedTestStorage.getClusterName());
        String secretCerts = getKafkaSecretCertificates(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as Secret certificates");
        assertThat(secretCerts, is(certs));
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("This test verifies that KafkaMirrorMaker2 status is correctly reported, including observed generation updates, URL information, and connector status during ready and not ready states."),
        steps = {
            @Step(value = "Deploy source Kafka cluster and KafkaMirrorMaker2.", expected = "KafkaMirrorMaker2 is ready with correct status information including URL and connector states."),
            @Step(value = "Modify KafkaMirrorMaker2 resources to cause NotReady state.", expected = "KafkaMirrorMaker2 becomes NotReady due to insufficient CPU resources."),
            @Step(value = "Restore KafkaMirrorMaker2 resources to recover.", expected = "KafkaMirrorMaker2 returns to Ready state with updated observed generation."),
            @Step(value = "Verify pod stability after recovery.", expected = "KafkaMirrorMaker2 pods remain stable without rolling updates.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.MIRROR_MAKER_2)
        }
    )
    void testKafkaMirrorMaker2Status() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String mm2Url = KafkaMirrorMaker2Resources.url(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 8083);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build());
        KubeResourceManager.get().createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName(), sharedTestStorage.getClusterName(), 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editOrNewHeartbeatConnector()
                        .withConfig(Map.of("heartbeats.topic.replication.factor", "1"))
                        .endHeartbeatConnector()
                    .endMirror()
                .endSpec()
                .build());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        assertKafkaMirrorMaker2Status(1, mm2Url, testStorage.getClusterName());

        // Corrupt MirrorMaker Pods
        KafkaMirrorMaker2Utils.replace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(),
            mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        // Restore MirrorMaker Pod
        KafkaMirrorMaker2Utils.replace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(),
            mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        assertKafkaMirrorMaker2Status(3, mm2Url, testStorage.getClusterName());
        // Wait for pods stability and check that pods weren't rolled
        PodUtils.verifyThatRunningPodsAreStable(Environment.TEST_SUITE_NAMESPACE, KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName()));
        assertKafkaMirrorMaker2Status(3, mm2Url, testStorage.getClusterName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @TestDoc(
        description = @Desc("This test verifies that KafkaMirrorMaker2 with invalid bootstrap server configuration fails gracefully and can be properly deleted."),
        steps = {
            @Step(value = "Create KafkaMirrorMaker2 with non-existing bootstrap servers.", expected = "KafkaMirrorMaker2 is created but becomes NotReady."),
            @Step(value = "Verify KafkaMirrorMaker2 remains in NotReady state.", expected = "KafkaMirrorMaker2 status shows NotReady due to invalid bootstrap configuration."),
            @Step(value = "Delete the KafkaMirrorMaker2 with invalid configuration.", expected = "KafkaMirrorMaker2 and its deployment are successfully deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.MIRROR_MAKER_2)
        }
    )
    void testKafkaMirrorMaker2WrongBootstrap() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String mirrorMaker2Name = testStorage.getClusterName() + "-mirror-maker-2";

        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Templates.kafkaMirrorMaker2(Environment.TEST_SUITE_NAMESPACE, mirrorMaker2Name,
            "non-existing-source", "non-existing-target", 1, false).build();

        KubeResourceManager.get().createResourceWithoutWait(kafkaMirrorMaker2);

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(Environment.TEST_SUITE_NAMESPACE, mirrorMaker2Name);

        // delete
        CrdClients.kafkaMirrorMaker2Client().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(mirrorMaker2Name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        DeploymentUtils.waitForDeploymentDeletion(Environment.TEST_SUITE_NAMESPACE, KafkaMirrorMaker2Resources.componentName(mirrorMaker2Name));
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
                .build()
            )
            .install();

        GenericKafkaListener plain = new GenericKafkaListenerBuilder()
                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();
        GenericKafkaListener tls = new GenericKafkaListenerBuilder()
                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();
        GenericKafkaListener nodePort = new GenericKafkaListenerBuilder()
                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        List<GenericKafkaListener> listeners;
        if (Environment.isNamespaceRbacScope()) {
            listeners = asList(plain, tls);
        } else {
            listeners = asList(plain, tls, nodePort);
        }

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
        );
        KafkaBuilder kafkaBuilder = KafkaTemplates.kafka(sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(listeners)
                .endKafka()
            .endSpec();

        KubeResourceManager.get().createResourceWithWait(kafkaBuilder.build());
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getTopicName(), sharedTestStorage.getClusterName()).build());
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        long observedGeneration = 0;

        KafkaStatus kafkaStatus = CrdClients.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get().getStatus();
        observedGeneration = CrdClients.kafkaNodePoolClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getBrokerPoolName()).get().getStatus().getObservedGeneration();

        assertThat("Kafka cluster status has incorrect Observed Generation", observedGeneration, is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getName()) {
                case TestConstants.TLS_LISTENER_DEFAULT_NAME:
                    assertThat("TLS bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("TLS bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case TestConstants.PLAIN_LISTENER_DEFAULT_NAME:
                    assertThat("Plain bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("Plain bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME:
                    Service extBootstrapService = KubeResourceManager.get().kubeClient().getClient().services()
                            .inNamespace(Environment.TEST_SUITE_NAMESPACE)
                            .withName(sharedTestStorage.getClusterName() + "-kafka-external-bootstrap")
                            .get();
                    assertThat("External bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("External bootstrap has incorrect host", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMaker2Status(long expectedObservedGeneration, String apiUrl, String mirrorMaker2Name) {
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = CrdClients.kafkaMirrorMaker2Client().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(mirrorMaker2Name).get().getStatus();
        assertThat("MirrorMaker2 cluster status has incorrect Observed Generation", kafkaMirrorMaker2Status.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("MirrorMaker2 cluster status has incorrect URL", kafkaMirrorMaker2Status.getUrl(), is(apiUrl));
        for (Map<String, Object> connector : kafkaMirrorMaker2Status.getConnectors()) {
            assertThat("One of the connectors is not RUNNING:\n" + connector.toString(), ((Map<String, String>) connector.get("connector")).get("state"), is("RUNNING"));
        }
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = CrdClients.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get().getStatus();
        assertThat("KafkaBridge cluster status has incorrect Observed Generation", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("KafkaBridge cluster status has incorrect URL", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = CrdClients.kafkaConnectClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get().getStatus();
        assertThat("KafkaConnect cluster status has incorrect Observed Generation", kafkaConnectStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("KafkaConnect cluster status has incorrect URL", kafkaConnectStatus.getUrl(), is(expectedUrl));

        validateConnectPlugins(kafkaConnectStatus.getConnectorPlugins());
    }

    void validateConnectPlugins(List<ConnectorPlugin> pluginsList) {
        assertThat(pluginsList, notNullValue());
        List<String> pluginsClasses = pluginsList.stream().map(p -> p.getConnectorClass()).collect(Collectors.toList());
        assertThat(pluginsClasses, hasItems("org.apache.kafka.connect.file.FileStreamSinkConnector",
                "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
                "org.apache.kafka.connect.mirror.MirrorSourceConnector"));
    }
}

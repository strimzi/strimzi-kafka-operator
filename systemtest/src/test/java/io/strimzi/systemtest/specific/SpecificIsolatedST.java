/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SPECIFIC;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(SPECIFIC)
@IsolatedSuite
public class SpecificIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(SpecificIsolatedST.class);

    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testLoadBalancerIpOverride(ExtensionContext extensionContext) {
        String bootstrapOverrideIP = "10.0.0.1";
        String brokerOverrideIP = "10.0.0.2";
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBootstrap()
                                    .withLoadBalancerIP(brokerOverrideIP)
                                .endBootstrap()
                                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                        .withBroker(0)
                                        .withLoadBalancerIP(brokerOverrideIP)
                                        .build())
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .build())
                .endKafka()
            .endSpec()
            .build());

        assertThat("Kafka External bootstrap doesn't contain correct loadBalancer address", kubeClient().getService(KafkaResources.externalBootstrapServiceName(clusterName)).getSpec().getLoadBalancerIP(), is(bootstrapOverrideIP));
        assertThat("Kafka Broker-0 service doesn't contain correct loadBalancer address", kubeClient().getService(KafkaResources.brokerSpecificService(clusterName, 0)).getSpec().getLoadBalancerIP(), is(brokerOverrideIP));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );
    }

    @Tag(REGRESSION)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testDeployUnsupportedKafka(ExtensionContext extensionContext) {
        String nonExistingVersion = "6.6.6";
        String nonExistingVersionMessage = "Unsupported Kafka.spec.kafka.version: " + nonExistingVersion + ". Supported versions are:.*";
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withVersion(nonExistingVersion)
                .endKafka()
            .endSpec().build());

        LOGGER.info("Kafka with version {} deployed.", nonExistingVersion);

        KafkaUtils.waitForKafkaNotReady(clusterOperator.getDeploymentNamespace(), clusterName);
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, clusterOperator.getDeploymentNamespace(), nonExistingVersionMessage);

        KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterName).delete();
    }

    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testLoadBalancerSourceRanges(ExtensionContext extensionContext) {
        String networkInterfaces = Exec.exec("ip", "route").out();
        Pattern ipv4InterfacesPattern = Pattern.compile("[0-9]+.[0-9]+.[0-9]+.[0-9]+\\/[0-9]+ dev (eth0|enp11s0u1).*");
        Matcher ipv4InterfacesMatcher = ipv4InterfacesPattern.matcher(networkInterfaces);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        ipv4InterfacesMatcher.find();
        LOGGER.info(ipv4InterfacesMatcher.group(0));
        String correctNetworkInterface = ipv4InterfacesMatcher.group(0);

        String[] correctNetworkInterfaceStrings = correctNetworkInterface.split(" ");

        String ipWithPrefix = correctNetworkInterfaceStrings[0];

        LOGGER.info("Network address of machine with associated prefix is {}", ipWithPrefix);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withTls(false)
                        .withNewConfiguration()
                            .withLoadBalancerSourceRanges(Collections.singletonList(ipWithPrefix))
                            .withFinalizers(LB_FINALIZERS)
                        .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );

        String invalidNetworkAddress = "255.255.255.111/30";

        LOGGER.info("Replacing Kafka CR invalid load-balancer source range to {}", invalidNetworkAddress);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
            kafka.getSpec().getKafka().setListeners(Collections.singletonList(new GenericKafkaListenerBuilder()
                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(false)
                .withNewConfiguration()
                    .withLoadBalancerSourceRanges(Collections.singletonList(ipWithPrefix))
                    .withFinalizers(LB_FINALIZERS)
                .endConfiguration()
                .build())),
                clusterOperator.getDeploymentNamespace()
        );

        LOGGER.info("Expecting that clients will not be able to connect to external load-balancer service cause of invalid load-balancer source range.");

        ExternalKafkaClient newExternalKafkaClient = externalKafkaClient.toBuilder()
            .withMessageCount(2 * MESSAGE_COUNT)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        assertThrows(TimeoutException.class, () ->
            newExternalKafkaClient.verifyProducedAndConsumedMessages(
                newExternalKafkaClient.sendMessagesPlain(),
                newExternalKafkaClient.receiveMessagesPlain()
            ));
    }

    @IsolatedTest
    @Tag(REGRESSION)
    void testClusterWideOperatorWithLimitedAccessToSpecificNamespaceViaRbacRole(final ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String namespaceWhereCreationOfCustomResourcesIsApproved = "example-1";

        // --- a) defining Role and ClusterRoles
        final Role strimziClusterOperator020 = TestUtils.configFromYaml(SetupClusterOperator.getInstanceHolder().switchClusterRolesToRolesIfNeeded(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml"), true), Role.class);

        // specify explicit namespace for Role (for ClusterRole we do not specify namespace because ClusterRole is a non-namespaced resource
        strimziClusterOperator020.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);

        final ClusterRole strimziClusterOperator021 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator022 = TestUtils.configFromYaml(SetupClusterOperator.getInstanceHolder().changeLeaseNameInResourceIfNeeded(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml").getAbsolutePath()), ClusterRole.class);
        final ClusterRole strimziClusterOperator023 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/023-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator030 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator031 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator033 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml"), ClusterRole.class);

        final List<Role> roles = Arrays.asList(strimziClusterOperator020);
        final List<ClusterRole> clusterRoles = Arrays.asList(strimziClusterOperator021, strimziClusterOperator022,
                strimziClusterOperator023, strimziClusterOperator030, strimziClusterOperator031, strimziClusterOperator033);

        // ---- b) defining RoleBindings
        final RoleBinding strimziClusterOperator020Namespaced = TestUtils.configFromYaml(SetupClusterOperator.getInstanceHolder().switchClusterRolesToRolesIfNeeded(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml"), true), RoleBinding.class);
        final RoleBinding strimziClusterOperator022LeaderElection = TestUtils.configFromYaml(SetupClusterOperator.getInstanceHolder().changeLeaseNameInResourceIfNeeded(new File(Constants.PATH_TO_LEASE_ROLE_BINDING).getAbsolutePath()), RoleBinding.class);

        // specify explicit namespace for RoleBindings
        strimziClusterOperator020Namespaced.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);
        strimziClusterOperator022LeaderElection.getMetadata().setNamespace(clusterOperator.getDeploymentNamespace());

        // reference Cluster Operator service account in RoleBindings
        strimziClusterOperator020Namespaced.getSubjects().stream().findFirst().get().setNamespace(clusterOperator.getDeploymentNamespace());
        strimziClusterOperator022LeaderElection.getSubjects().stream().findFirst().get().setNamespace(clusterOperator.getDeploymentNamespace());

        final List<RoleBinding> roleBindings = Arrays.asList(
                strimziClusterOperator020Namespaced,
                strimziClusterOperator022LeaderElection
        );

        // ---- c) defining ClusterRoleBindings
        final List<ClusterRoleBinding> clusterRoleBindings = Arrays.asList(
            ClusterRoleBindingTemplates.getClusterOperatorWatchedCrb(clusterOperator.getClusterOperatorName(), clusterOperator.getDeploymentNamespace()),
            ClusterRoleBindingTemplates.getClusterOperatorEntityOperatorCrb(clusterOperator.getClusterOperatorName(), clusterOperator.getDeploymentNamespace())
        );

        clusterOperator.unInstall();
        // create namespace, where we will be able to deploy Custom Resources
        cluster.createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName()), namespaceWhereCreationOfCustomResourcesIsApproved);
        clusterOperator = clusterOperator.defaultInstallation()
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            // use our pre-defined Roles
            .withRoles(roles)
            // use our pre-defined RoleBindings
            .withRoleBindings(roleBindings)
            // use our pre-defined ClusterRoles
            .withClusterRoles(clusterRoles)
            // use our pre-defined ClusterRoleBindings
            .withClusterRoleBindings(clusterRoleBindings)
            .createInstallation()
            .runBundleInstallation();

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editMetadata()
                // this should not work
                    .withNamespace(clusterOperator.getDeploymentNamespace())
                .endMetadata()
                .build());

        // implicit verification that a user is able to deploy Kafka cluster in namespace <example-1>, where we are allowed
        // to create Custom Resources because of `*-namespaced Role`
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editMetadata()
                // this should work
                    .withNamespace(namespaceWhereCreationOfCustomResourcesIsApproved)
                .endMetadata()
                .build());

        // verify that in `infra-namespace` we are not able to deploy Kafka cluster
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getClusterName(), clusterOperator.getDeploymentNamespace(),
                ".*Forbidden!Configured service account doesn't have access.*");

        final Condition condition = KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().findFirst().get();

        assertThat(condition.getReason(), CoreMatchers.is("KubernetesClientException"));
        assertThat(condition.getStatus(), CoreMatchers.is("True"));

        // rollback to the default configuration
        clusterOperator.rollbackToDefaultConfiguration();
    }

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
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

        KafkaResource.replaceKafkaResource(clusterName, kafka ->
            kafka.getSpec().getKafka().setListeners(Collections.singletonList(new GenericKafkaListenerBuilder()
                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(false)
                .withNewConfiguration()
                    .withLoadBalancerSourceRanges(Collections.singletonList(ipWithPrefix))
                    .withFinalizers(LB_FINALIZERS)
                .endConfiguration()
                .build()))
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

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}

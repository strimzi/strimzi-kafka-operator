/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CO_OPERATION_TIMEOUT_SHORT;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SPECIFIC;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(SPECIFIC)
public class SpecificST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(SpecificST.class);
    public static final String NAMESPACE = "specific-cluster-test";

    @Test
    @Tag(REGRESSION)
    @Tag(INTERNAL_CLIENTS_USED)
    void testRackAware() {
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String rackKey = "rack-key";
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                .endKafka()
            .endSpec().done();

        Affinity kafkaPodSpecAffinity = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getSpec().getTemplate().getSpec().getAffinity();
        NodeSelectorRequirement kafkaPodNodeSelectorRequirement = kafkaPodSpecAffinity.getNodeAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);

        assertThat(kafkaPodNodeSelectorRequirement.getKey(), is(rackKey));
        assertThat(kafkaPodNodeSelectorRequirement.getOperator(), is("Exists"));

        PodAffinityTerm kafkaPodAffinityTerm = kafkaPodSpecAffinity.getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPodAffinityTerm();

        assertThat(kafkaPodAffinityTerm.getTopologyKey(), is(rackKey));
        assertThat(kafkaPodAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/cluster", CLUSTER_NAME));
        assertThat(kafkaPodAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/name", KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)));

        String rackId = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "cat /opt/kafka/init/rack.id").out();
        assertThat(rackId.trim(), is("zone"));

        String brokerRack = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out();
        assertThat(brokerRack.contains("broker.rack=zone"), is(true));

        String uid = kubeClient().getPodUid(KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        List<Event> events = kubeClient().listEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(0)
            .build();

        kafkaBasicClientJob.producerStrimzi().done();
        kafkaBasicClientJob.consumerStrimzi().done();
    }

    @Test
    @Tag(CONNECT)
    @Tag(REGRESSION)
    @Tag(INTERNAL_CLIENTS_USED)
    void testRackAwareConnectWrongDeployment() {
        // We need to update CO configuration to set OPERATION_TIMEOUT to shorter value, because we expect timeout in that test
        Map<String, String> coSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        // We have to install CO in class stack, otherwise it will be deleted at the end of test case and all following tests will fail
        ResourceManager.setClassResources();
        BundleResource.clusterOperator(NAMESPACE, CO_OPERATION_TIMEOUT_SHORT).done();
        // Now we set pointer stack to method again
        ResourceManager.setMethodResources();
        coSnapshot = DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coSnapshot);

        String wrongRackKey = "wrong-key";
        String rackKey = "rack-key";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewRack()
                            .withTopologyKey(rackKey)
                        .endRack()
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec().done();

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

        LOGGER.info("Deploy KafkaConnect with wrong rack-aware topology key: {}", wrongRackKey);
        KafkaConnect kc = KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.defaultKafkaConnect(CLUSTER_NAME, CLUSTER_NAME, 1)
                .editSpec()
                    .withNewRack()
                        .withTopologyKey(wrongRackKey)
                    .endRack()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .endSpec().build());
        KafkaConnectResource.allowNetworkPolicyForKafkaConnect(kc);

        PodUtils.waitForPendingPod(CLUSTER_NAME + "-connect");
        List<String> connectWrongPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND);
        String connectWrongPodName = connectWrongPods.get(0);
        LOGGER.info("Waiting for ClusterOperator to get timeout operation of incorrectly set up KafkaConnect");
        KafkaConnectUtils.waitForKafkaConnectCondition("TimeoutException", "NotReady", NAMESPACE, CLUSTER_NAME);

        kc = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get();
        PodStatus kcWrongStatus = kubeClient().getPod(connectWrongPodName).getStatus();
        assertThat("Unschedulable", is(kcWrongStatus.getConditions().get(0).getReason()));
        assertThat("PodScheduled", is(kcWrongStatus.getConditions().get(0).getType()));

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kafkaConnect -> {
            kafkaConnect.getSpec().setRack(new Rack(rackKey));
        });
        KafkaConnectUtils.waitForConnectReady(CLUSTER_NAME);
        LOGGER.info("KafkaConnect is ready with changed rack key: '{}'.", rackKey);
        LOGGER.info("Verify KafkaConnect rack key update");
        kc = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get();
        assertThat(kc.getSpec().getRack().getTopologyKey(), is(rackKey));

        List<String> kcPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND);
        KafkaConnectUtils.sendReceiveMessagesThroughConnect(kcPods.get(0), TOPIC_NAME, kafkaClientsPodName, NAMESPACE, CLUSTER_NAME);

        // Revert changes for CO deployment
        ResourceManager.setClassResources();
        BundleResource.clusterOperator(NAMESPACE).done();
        ResourceManager.setMethodResources();
        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coSnapshot);
    }

    @Test
    @Tag(CONNECT)
    @Tag(REGRESSION)
    @Tag(INTERNAL_CLIENTS_USED)
    public void testRackAwareConnectCorrectDeployment() {
        // We need to update CO configuration to set OPERATION_TIMEOUT to shorter value, because we expect timeout in that test
        Map<String, String> coSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        // We have to install CO in class stack, otherwise it will be deleted at the end of test case and all following tests will fail
        ResourceManager.setClassResources();
        BundleResource.clusterOperator(NAMESPACE, CO_OPERATION_TIMEOUT_SHORT).done();
        // Now we set pointer stack to method again
        ResourceManager.setMethodResources();
        coSnapshot = DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coSnapshot);

        String rackKey = "rack-key";
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewRack()
                            .withTopologyKey(rackKey)
                        .endRack()
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec().done();

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

        LOGGER.info("Deploy KafkaConnect with correct rack-aware topology key: {}", rackKey);
        KafkaConnect kc = KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editSpec()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .endSpec().done();
        KafkaConnectResource.allowNetworkPolicyForKafkaConnect(kc);

        String topicName = "topic-test-rack-aware";
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        List<String> connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND);
        for (String connectPodName : connectPods) {
            Affinity connectPodSpecAffinity = kubeClient().getDeployment(KafkaConnectResources.deploymentName(CLUSTER_NAME)).getSpec().getTemplate().getSpec().getAffinity();
            NodeSelectorRequirement connectPodNodeSelectorRequirement = connectPodSpecAffinity.getNodeAffinity()
                    .getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
            Pod connectPod = kubeClient().getPod(connectPodName);
            NodeAffinity nodeAffinity = connectPod.getSpec().getAffinity().getNodeAffinity();

            LOGGER.info("PodName: {}\nNodeAffinity: {}", connectPodName, nodeAffinity);
            assertThat(connectPodNodeSelectorRequirement.getKey(), is(rackKey));
            assertThat(connectPodNodeSelectorRequirement.getOperator(), is("Exists"));

            KafkaConnectUtils.sendReceiveMessagesThroughConnect(connectPodName, topicName, kafkaClientsPodName, NAMESPACE, CLUSTER_NAME);
        }

        // Revert changes for CO deployment
        ResourceManager.setClassResources();
        BundleResource.clusterOperator(NAMESPACE).done();
        ResourceManager.setMethodResources();
        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coSnapshot);
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerIpOverride() {
        String bootstrapOverrideIP = "10.0.0.1";
        String brokerOverrideIP = "10.0.0.2";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        assertThat("Kafka External bootstrap doesn't contain correct loadBalancer address", kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME)).getSpec().getLoadBalancerIP(), is(bootstrapOverrideIP));
        assertThat("Kafka Broker-0 service doesn't contain correct loadBalancer address", kubeClient().getService(KafkaResources.brokerSpecificService(CLUSTER_NAME, 0)).getSpec().getLoadBalancerIP(), is(brokerOverrideIP));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    @Tag(REGRESSION)
    void testDeployUnsupportedKafka() {
        String nonExistingVersion = "6.6.6";
        String nonExistingVersionMessage = "Version " + nonExistingVersion + " is not supported. Supported versions are.*";

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withVersion(nonExistingVersion)
                .endKafka()
            .endSpec().build());

        LOGGER.info("Kafka with version {} deployed.", nonExistingVersion);

        KafkaUtils.waitForKafkaNotReady(CLUSTER_NAME);
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE, nonExistingVersionMessage);

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerSourceRanges() {
        String networkInterfaces = Exec.exec("ip", "route").out();
        Pattern ipv4InterfacesPattern = Pattern.compile("[0-9]+.[0-9]+.[0-9]+.[0-9]+\\/[0-9]+ dev (eth0|enp11s0u1).*");
        Matcher ipv4InterfacesMatcher = ipv4InterfacesPattern.matcher(networkInterfaces);

        ipv4InterfacesMatcher.find();
        LOGGER.info(ipv4InterfacesMatcher.group(0));
        String correctNetworkInterface = ipv4InterfacesMatcher.group(0);

        String[] correctNetworkInterfaceStrings = correctNetworkInterface.split(" ");

        String ipWithPrefix = correctNetworkInterfaceStrings[0];

        LOGGER.info("Network address of machine with associated prefix is {}", ipWithPrefix);

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(false)
                            .withNewConfiguration()
                                .withLoadBalancerSourceRanges(Collections.singletonList(ipWithPrefix))
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );

        String invalidNetworkAddress = "255.255.255.111/30";

        LOGGER.info("Replacing Kafka CR invalid load-balancer source range to {}", invalidNetworkAddress);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka ->
                kafka.getSpec().getKafka().getTemplate().getExternalBootstrapService().setLoadBalancerSourceRanges(Collections.singletonList(invalidNetworkAddress))
        );

        LOGGER.info("Expecting that clients will not be able to connect to external load-balancer service cause of invalid load-balancer source range.");

        BasicExternalKafkaClient newBasicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withMessageCount(2 * MESSAGE_COUNT)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        assertThrows(TimeoutException.class, () ->
            newBasicExternalKafkaClient.verifyProducedAndConsumedMessages(
                newBasicExternalKafkaClient.sendMessagesPlain(),
                newBasicExternalKafkaClient.receiveMessagesPlain()
            ));
    }

    @BeforeAll
    void setup() {
        LOGGER.info(BridgeUtils.getBridgeVersion());
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 060-Deployment
        BundleResource.clusterOperator(NAMESPACE).done();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBootstrapOverride;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBootstrapOverrideBuilder;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverrideBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.systemtest.MessagingBaseST;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SPECIFIC;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(SPECIFIC)
public class SpecificST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(SpecificST.class);
    public static final String NAMESPACE = "specific-cluster-test";

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testRackAware() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                .withNewRack()
                    .withTopologyKey("rack-key")
                .endRack()
                .editListeners()
                    .withNewKafkaListenerExternalLoadBalancer()
                        .withTls(false)
                    .endKafkaListenerExternalLoadBalancer()
                .endListeners()
                .endKafka()
            .endSpec().done();

        String rackId = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "cat /opt/kafka/init/rack.id").out();
        assertThat(rackId.trim(), is("zone"));

        String brokerRack = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out();
        assertThat(brokerRack.contains("broker.rack=zone"), is(true));

        String uid = kubeClient().getPodUid(KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        List<Event> events = kubeClient().listEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        waitForClusterAvailability(NAMESPACE);
    }


    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testLoadBalancerIpOverride() throws Exception {
        String bootstrapOverrideIP = "10.0.0.1";
        String brokerOverrideIP = "10.0.0.2";

        LoadBalancerListenerBootstrapOverride bootstrapOverride = new LoadBalancerListenerBootstrapOverrideBuilder()
                .withLoadBalancerIP(bootstrapOverrideIP)
                .build();

        LoadBalancerListenerBrokerOverride brokerOverride0 = new LoadBalancerListenerBrokerOverrideBuilder()
                .withBroker(0)
                .withLoadBalancerIP(brokerOverrideIP)
                .build();

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .withNewOverrides()
                            .withBootstrap(bootstrapOverride)
                            .withBrokers(brokerOverride0)
                        .endOverrides()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        assertThat("Kafka External bootstrap doesn't contain correct loadBalancer address", kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME)).getSpec().getLoadBalancerIP(), is(bootstrapOverrideIP));
        assertThat("Kafka Broker-0 service doesn't contain correct loadBalancer address", kubeClient().getService(KafkaResources.brokerSpecificService(CLUSTER_NAME, 0)).getSpec().getLoadBalancerIP(), is(brokerOverrideIP));

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    @Tag(REGRESSION)
    void testDeployUnsupportedKafka() {
        testMethodResources().kafkaWithoutWait(testMethodResources().defaultKafka(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withVersion("6.6.6")
                .endKafka()
            .endSpec().build());

        LOGGER.info("Wait until Zookeeper stateful set is ready");
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 1);

        LOGGER.info("Find warning about Kafka version in Cluster Operator log");
        String expectedLog = "createOrUpdate failed. Version 6.6.6 is not supported. Supported versions are:";
        String coPodName = kubeClient().getClusterOperatorPodName();
        StUtils.waitUntilMessageIsInLogs(coPodName, "strimzi-cluster-operator", expectedLog);
        LOGGER.info("Warning is present in Cluster Operator log, going to teardown Created Kafka");

        assertThat(kubeClient().logs(coPodName), not(containsString("NullPointer")));
        assertKafkaStatus("NotReady", "Version 6.6.6 is not supported");
    }

    // This should be moved to StUtils in the futures
    void assertKafkaStatus(String status, String message) {
        KafkaStatus kafkaStatus = testMethodResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Kafka cluster status should be " + status, kafkaStatus.getConditions().get(0).getType(), is(status));
        assertThat("Kafka cluster status message should be different", kafkaStatus.getConditions().get(0).getMessage(), containsString(message));
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @AfterEach
    void deleteTestResources() {
        deleteTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
    }
}

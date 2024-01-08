/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class ListenersUtilsTest {
    private final GenericKafkaListener oldPlain = new GenericKafkaListenerBuilder()
            .withName("plain")
            .withPort(9092)
            .withType(KafkaListenerType.INTERNAL)
            .withTls(false)
            .build();

    private final GenericKafkaListener oldTls = new GenericKafkaListenerBuilder()
            .withName("tls")
            .withPort(9093)
            .withType(KafkaListenerType.INTERNAL)
            .withTls(true)
            .build();

    private final GenericKafkaListener oldExternal = new GenericKafkaListenerBuilder()
            .withName("external")
            .withPort(9094)
            .withType(KafkaListenerType.ROUTE)
            .withTls(true)
            .build();

    private final GenericKafkaListener newPlain = new GenericKafkaListenerBuilder()
            .withName("plain2")
            .withPort(9900)
            .withType(KafkaListenerType.INTERNAL)
            .withTls(false)
            .withNewConfiguration()
                .withUseServiceDnsDomain(true)
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newTls = new GenericKafkaListenerBuilder()
            .withName("tls2")
            .withPort(9901)
            .withType(KafkaListenerType.INTERNAL)
            .withTls(true)
            .withNewConfiguration()
                .withNewBrokerCertChainAndKey()
                    .withCertificate("cert")
                    .withKey("key")
                    .withSecretName("secretName")
                .endBrokerCertChainAndKey()
                .withUseServiceDnsDomain(true)
                .withNewBootstrap()
                    .withAlternativeNames(asList("my-name-1", "my-name-2"))
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newRoute = new GenericKafkaListenerBuilder()
            .withName("route")
            .withPort(9902)
            .withType(KafkaListenerType.ROUTE)
            .withTls(true)
            .withNewConfiguration()
                .withNewBootstrap()
                    .withAlternativeNames(asList("my-route-1", "my-route-2"))
                    .withHost("my-route-host")
                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .withLabels(Collections.singletonMap("label", "label-value"))
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withHost("my-route-host-1")
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .withLabels(Collections.singletonMap("label", "label-value"))
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withHost("my-route-host-2")
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .withLabels(Collections.singletonMap("label", "label-value"))
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newNodePort = new GenericKafkaListenerBuilder()
            .withName("np1")
            .withPort(9903)
            .withType(KafkaListenerType.NODEPORT)
            .withTls(true)
            .withNewConfiguration()
            .endConfiguration()
            .build();

    private final GenericKafkaListener newNodePort2 = new GenericKafkaListenerBuilder()
            .withName("np2")
            .withPort(9904)
            .withType(KafkaListenerType.NODEPORT)
            .withTls(true)
            .withNewConfiguration()
                .withExternalTrafficPolicy(ExternalTrafficPolicy.CLUSTER)
                .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                .withNewBootstrap()
                    .withAlternativeNames(asList("my-np-1", "my-np-2"))
                    .withNodePort(32189)
                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .withLabels(Collections.singletonMap("label", "label-value"))
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withNodePort(32190)
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .withLabels(Collections.singletonMap("label", "label-value"))
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withNodePort(32191)
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .withLabels(Collections.singletonMap("label", "label-value"))
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newLoadBalancer = new GenericKafkaListenerBuilder()
            .withName("lb1")
            .withPort(9905)
            .withType(KafkaListenerType.LOADBALANCER)
            .withTls(true)
            .withNewConfiguration()
                .withNewBootstrap()
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newLoadBalancer2 = new GenericKafkaListenerBuilder()
            .withName("lb2")
            .withPort(9906)
            .withType(KafkaListenerType.LOADBALANCER)
            .withTls(true)
            .withNewConfiguration()
                .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                .withLoadBalancerSourceRanges(asList("10.0.0.0/8", "130.211.204.1/32"))
                .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                .withNewBootstrap()
                    .withAlternativeNames(asList("my-lb-1", "my-lb-2"))
                    .withLoadBalancerIP("130.211.204.1")
                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .withLabels(Collections.singletonMap("label", "label-value"))
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host-1")
                                .withAdvertisedPort(19092)
                                .withLoadBalancerIP("130.211.204.1")
                                .withAnnotations(Collections.singletonMap("dns-anno-1", "dns-value"))
                                .withLabels(Collections.singletonMap("label-1", "label-value"))
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host-2")
                                .withAdvertisedPort(29092)
                                .withLoadBalancerIP("130.211.204.1")
                                .withAnnotations(Collections.singletonMap("dns-anno-2", "dns-value"))
                                .withLabels(Collections.singletonMap("label-2", "label-value"))
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newIngress = new GenericKafkaListenerBuilder()
            .withName("ing1")
            .withPort(9907)
            .withType(KafkaListenerType.INGRESS)
            .withTls(true)
            .withNewConfiguration()
                .withNewBootstrap()
                    .withHost("my-host")
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withHost("my-host-1")
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withHost("my-host-2")
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newIngress2 = new GenericKafkaListenerBuilder()
            .withName("ing2")
            .withPort(9908)
            .withType(KafkaListenerType.INGRESS)
            .withTls(true)
            .withNewConfiguration()
                .withControllerClass("my-ingress")
                .withNewBootstrap()
                    .withAlternativeNames(asList("my-ing-1", "my-ing-2"))
                    .withHost("my-ing-host")
                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .withLabels(Collections.singletonMap("label", "label-value"))
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withHost("my-host")
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .withLabels(Collections.singletonMap("label", "label-value"))
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withHost("my-host")
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .withLabels(Collections.singletonMap("label", "label-value"))
                                .build())
            .endConfiguration()
            .build();

    private final GenericKafkaListener newNodePort3 = new GenericKafkaListenerBuilder()
            .withName("np3")
            .withPort(9909)
            .withType(KafkaListenerType.NODEPORT)
            .withTls(true)
            .withNewConfiguration()
                .withNewBootstrap()
                .endBootstrap()
                .withBrokers()
            .endConfiguration()
            .build();

    private final GenericKafkaListener newClusterIP = new GenericKafkaListenerBuilder()
            .withName("clusterIP")
            .withPort(9907)
            .withType(KafkaListenerType.CLUSTER_IP)
            .withTls(false)
            .withNewConfiguration()
            .withNewBootstrap()
            .withHost("my-host")
            .endBootstrap()
            .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                            .withBroker(0)
                            .withHost("my-host-1")
                            .build(),
                    new GenericKafkaListenerConfigurationBrokerBuilder()
                            .withBroker(1)
                            .withHost("my-host-2")
                            .build())
            .endConfiguration()
            .build();

    List<GenericKafkaListener> simpleListeners = asList(oldPlain, oldTls, oldExternal, newNodePort, newLoadBalancer, newIngress, newClusterIP);
    List<GenericKafkaListener> internalListeners = asList(oldPlain, oldTls, newPlain, newTls);
    List<GenericKafkaListener> allListeners = asList(oldPlain, oldTls, oldExternal, newPlain, newTls, newRoute,
            newNodePort, newNodePort2, newNodePort3, newLoadBalancer, newLoadBalancer2, newIngress, newIngress2, newClusterIP);

    @ParallelTest
    public void testInternalListeners()    {
        assertThat(ListenersUtils.internalListeners(allListeners), hasSize(4));
        assertThat(ListenersUtils.internalListeners(allListeners).stream().map(GenericKafkaListener::getName).collect(Collectors.toList()),
                containsInAnyOrder("plain", "tls", "plain2", "tls2"));
    }

    @ParallelTest
    public void testNodePortListeners()    {
        assertThat(ListenersUtils.nodePortListeners(allListeners), hasSize(3));
        assertThat(ListenersUtils.nodePortListeners(allListeners).stream().map(GenericKafkaListener::getName).collect(Collectors.toList()),
                containsInAnyOrder("np1", "np2", "np3"));
        assertThat(ListenersUtils.hasNodePortListener(allListeners), is(true));

        assertThat(ListenersUtils.nodePortListeners(internalListeners), hasSize(0));
        assertThat(ListenersUtils.hasNodePortListener(internalListeners), is(false));
    }

    @ParallelTest
    public void testIngressListeners()    {
        assertThat(ListenersUtils.ingressListeners(allListeners), hasSize(2));
        assertThat(ListenersUtils.ingressListeners(allListeners).stream().map(GenericKafkaListener::getName).collect(Collectors.toList()),
                containsInAnyOrder("ing1", "ing2"));
        assertThat(ListenersUtils.hasIngressListener(allListeners), is(true));

        assertThat(ListenersUtils.ingressListeners(internalListeners), hasSize(0));
        assertThat(ListenersUtils.hasIngressListener(internalListeners), is(false));
    }

    @ParallelTest
    public void testClusterIPListeners()    {
        assertThat(ListenersUtils.clusterIPListeners(allListeners), hasSize(1));
        assertThat(ListenersUtils.clusterIPListeners(allListeners).stream().map(GenericKafkaListener::getName).collect(Collectors.toList()),
                containsInAnyOrder("clusterIP"));
        assertThat(ListenersUtils.hasClusterIPListener(allListeners), is(true));

        assertThat(ListenersUtils.clusterIPListeners(internalListeners), hasSize(0));
        assertThat(ListenersUtils.hasClusterIPListener(internalListeners), is(false));
    }

    @ParallelTest
    public void testAlternativeNames()  {
        assertThat(ListenersUtils.alternativeNames(simpleListeners), hasSize(0));

        assertThat(ListenersUtils.alternativeNames(simpleListeners), hasSize(0));
        assertThat(ListenersUtils.alternativeNames(allListeners),
                containsInAnyOrder("my-name-1", "my-name-2", "my-lb-1", "my-lb-2", "my-route-1", "my-route-2", "my-np-1", "my-np-2", "my-ing-1", "my-ing-2"));
    }

    @ParallelTest
    public void testIdentifier()    {
        assertThat(ListenersUtils.identifier(oldPlain), is("plain-9092"));
    }

    @ParallelTest
    public void testEnvVarIdentifier()    {
        assertThat(ListenersUtils.envVarIdentifier(oldPlain), is("PLAIN_9092"));
    }

    @ParallelTest
    public void testBackwardsCompatiblePortName()    {
        assertThat(ListenersUtils.backwardsCompatiblePortName(oldPlain), is("tcp-clients"));
        assertThat(ListenersUtils.backwardsCompatiblePortName(oldTls), is("tcp-clientstls"));
        assertThat(ListenersUtils.backwardsCompatiblePortName(oldExternal), is("tcp-external"));
        assertThat(ListenersUtils.backwardsCompatiblePortName(newPlain), is("tcp-plain2"));
        assertThat(ListenersUtils.backwardsCompatiblePortName(newTls), is("tcp-tls2"));
        assertThat(ListenersUtils.backwardsCompatiblePortName(newLoadBalancer), is("tcp-lb1"));
    }

    @ParallelTest
    public void testBackwardsCompatibleServiceNames()    {
        String clusterName = "my-cluster";

        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, oldPlain), is(clusterName + "-kafka-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, oldTls), is(clusterName + "-kafka-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, oldExternal), is(clusterName + "-kafka-external-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, newPlain), is(clusterName + "-kafka-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, newTls), is(clusterName + "-kafka-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, newLoadBalancer), is(clusterName + "-kafka-lb1-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapServiceName(clusterName, newNodePort), is(clusterName + "-kafka-np1-bootstrap"));
    }

    @ParallelTest
    public void testBackwardsCompatibleBootstrapRouteOrIngressName()    {
        String clusterName = "my-cluster";

        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, oldPlain));
        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, oldTls));
        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, newPlain));
        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, newTls));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, oldExternal), is(clusterName + "-kafka-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, newLoadBalancer), is(clusterName + "-kafka-lb1-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, newNodePort), is(clusterName + "-kafka-np1-bootstrap"));
        assertThat(ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(clusterName, newRoute), is(clusterName + "-kafka-route-bootstrap"));
    }

    @ParallelTest
    public void testBackwardsCompatibleBrokerServiceName()    {
        String componentName = "my-cluster-kafka";

        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, oldPlain));
        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, oldTls));
        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, newPlain));
        assertThrows(UnsupportedOperationException.class, () -> ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, newTls));
        assertThat(ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, oldExternal), is(componentName + "-1"));
        assertThat(ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, newLoadBalancer), is(componentName + "-lb1-1"));
        assertThat(ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, newNodePort), is(componentName + "-np1-1"));
        assertThat(ListenersUtils.backwardsCompatiblePerBrokerServiceName(componentName, 1, newRoute), is(componentName + "-route-1"));
    }

    @ParallelTest
    public void testBootstrapNodePort() {
        assertThat(ListenersUtils.bootstrapNodePort(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.bootstrapNodePort(newNodePort2), is(32189));
        assertThat(ListenersUtils.bootstrapNodePort(newNodePort3), is(nullValue()));
        assertThat(ListenersUtils.bootstrapNodePort(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.bootstrapNodePort(newTls), is(nullValue()));
        assertThat(ListenersUtils.bootstrapNodePort(newLoadBalancer), is(nullValue()));
    }

    @ParallelTest
    public void testBrokerNodePort() {
        assertThat(ListenersUtils.brokerNodePort(newNodePort, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerNodePort(newNodePort2, 0), is(32190));
        assertThat(ListenersUtils.brokerNodePort(newNodePort2, 1), is(32191));
        assertThat(ListenersUtils.brokerNodePort(newNodePort2, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerNodePort(newNodePort3, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerNodePort(oldPlain, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerNodePort(newTls, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerNodePort(newLoadBalancer, 1), is(nullValue()));
    }

    @ParallelTest
    public void testBootstrapLoadBalancerIP() {
        assertThat(ListenersUtils.bootstrapLoadBalancerIP(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.bootstrapLoadBalancerIP(newLoadBalancer2), is("130.211.204.1"));
        assertThat(ListenersUtils.bootstrapLoadBalancerIP(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.bootstrapLoadBalancerIP(newTls), is(nullValue()));
        assertThat(ListenersUtils.bootstrapLoadBalancerIP(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.bootstrapLoadBalancerIP(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testBrokerLoadBalancerIP() {
        assertThat(ListenersUtils.brokerLoadBalancerIP(newLoadBalancer, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerLoadBalancerIP(newLoadBalancer2, 0), is("130.211.204.1"));
        assertThat(ListenersUtils.brokerLoadBalancerIP(newLoadBalancer2, 1), is("130.211.204.1"));
        assertThat(ListenersUtils.brokerLoadBalancerIP(newLoadBalancer2, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerLoadBalancerIP(oldPlain, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerLoadBalancerIP(newTls, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerLoadBalancerIP(newNodePort, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerLoadBalancerIP(newNodePort3, 1), is(nullValue()));
    }

    @ParallelTest
    public void testBootstrapLabelsAndAnnotations() {
        assertThat(ListenersUtils.bootstrapAnnotations(newLoadBalancer), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapAnnotations(newLoadBalancer2), is(Collections.singletonMap("dns-anno", "dns-value")));
        assertThat(ListenersUtils.bootstrapAnnotations(oldPlain), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapAnnotations(newTls), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapAnnotations(newNodePort), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapAnnotations(newNodePort3), is(emptyMap()));

        assertThat(ListenersUtils.bootstrapLabels(newLoadBalancer), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapLabels(newLoadBalancer2), is(Collections.singletonMap("label", "label-value")));
        assertThat(ListenersUtils.bootstrapLabels(oldPlain), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapLabels(newTls), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapLabels(newNodePort), is(emptyMap()));
        assertThat(ListenersUtils.bootstrapLabels(newNodePort3), is(emptyMap()));
    }

    @ParallelTest
    public void testBrokerLabelsAndAnnotations() {
        assertThat(ListenersUtils.brokerAnnotations(newLoadBalancer, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerAnnotations(newLoadBalancer2, 0), is(Collections.singletonMap("dns-anno-1", "dns-value")));
        assertThat(ListenersUtils.brokerAnnotations(newLoadBalancer2, 1), is(Collections.singletonMap("dns-anno-2", "dns-value")));
        assertThat(ListenersUtils.brokerAnnotations(newLoadBalancer2, 2), is(emptyMap()));
        assertThat(ListenersUtils.brokerAnnotations(oldPlain, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerAnnotations(newTls, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerAnnotations(newNodePort, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerAnnotations(newNodePort3, 1), is(emptyMap()));

        assertThat(ListenersUtils.brokerLabels(newLoadBalancer, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerLabels(newLoadBalancer2, 0), is(Collections.singletonMap("label-1", "label-value")));
        assertThat(ListenersUtils.brokerLabels(newLoadBalancer2, 1), is(Collections.singletonMap("label-2", "label-value")));
        assertThat(ListenersUtils.brokerLabels(newLoadBalancer2, 2), is(emptyMap()));
        assertThat(ListenersUtils.brokerLabels(oldPlain, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerLabels(newTls, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerLabels(newNodePort, 1), is(emptyMap()));
        assertThat(ListenersUtils.brokerLabels(newNodePort3, 1), is(emptyMap()));
    }

    @ParallelTest
    public void testBootstrapHost() {
        assertThat(ListenersUtils.bootstrapHost(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.bootstrapHost(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.bootstrapHost(newRoute), is("my-route-host"));
        assertThat(ListenersUtils.bootstrapHost(newIngress), is("my-host"));
        assertThat(ListenersUtils.bootstrapHost(newClusterIP), is("my-host"));
        assertThat(ListenersUtils.bootstrapHost(newIngress2), is("my-ing-host"));
        assertThat(ListenersUtils.bootstrapHost(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.bootstrapHost(newTls), is(nullValue()));
        assertThat(ListenersUtils.bootstrapHost(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.bootstrapHost(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testBrokerHost() {
        assertThat(ListenersUtils.brokerHost(newLoadBalancer, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(oldExternal, 0), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(newRoute, 0), is("my-route-host-1"));
        assertThat(ListenersUtils.brokerHost(newRoute, 1), is("my-route-host-2"));
        assertThat(ListenersUtils.brokerHost(newRoute, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(newIngress, 0), is("my-host-1"));
        assertThat(ListenersUtils.brokerHost(newIngress, 1), is("my-host-2"));
        assertThat(ListenersUtils.brokerHost(newIngress, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(newClusterIP, 0), is("my-host-1"));
        assertThat(ListenersUtils.brokerHost(newClusterIP, 1), is("my-host-2"));
        assertThat(ListenersUtils.brokerHost(newClusterIP, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(oldPlain, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(newTls, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(newNodePort, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerHost(newNodePort3, 1), is(nullValue()));
    }

    @ParallelTest
    public void testBrokerAdvertisedHost() {
        assertThat(ListenersUtils.brokerAdvertisedHost(newLoadBalancer, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(oldExternal, 0), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(newLoadBalancer2, 0), is("advertised-host-1"));
        assertThat(ListenersUtils.brokerAdvertisedHost(newLoadBalancer2, 1), is("advertised-host-2"));
        assertThat(ListenersUtils.brokerAdvertisedHost(newLoadBalancer2, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(oldPlain, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(newTls, 1), is("advertised-host"));
        assertThat(ListenersUtils.brokerAdvertisedHost(newNodePort, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(newNodePort3, 1), is(nullValue()));
    }

    @ParallelTest
    public void testBrokerAdvertisedPort() {
        assertThat(ListenersUtils.brokerAdvertisedPort(newLoadBalancer, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedPort(oldExternal, 0), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedPort(newLoadBalancer2, 0), is(19092));
        assertThat(ListenersUtils.brokerAdvertisedPort(newLoadBalancer2, 1), is(29092));
        assertThat(ListenersUtils.brokerAdvertisedPort(newLoadBalancer2, 2), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedPort(oldPlain, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedPort(newTls, 1), is(9092));
        assertThat(ListenersUtils.brokerAdvertisedPort(newNodePort, 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedPort(newNodePort3, 1), is(nullValue()));
    }

    @ParallelTest
    public void testLoadBalancerSourceRanges() {
        assertThat(ListenersUtils.loadBalancerSourceRanges(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.loadBalancerSourceRanges(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.loadBalancerSourceRanges(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.loadBalancerSourceRanges(newLoadBalancer2), containsInAnyOrder("10.0.0.0/8", "130.211.204.1/32"));
        assertThat(ListenersUtils.loadBalancerSourceRanges(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.loadBalancerSourceRanges(newTls), is(nullValue()));
        assertThat(ListenersUtils.loadBalancerSourceRanges(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.loadBalancerSourceRanges(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testFinalizers() {
        assertThat(ListenersUtils.finalizers(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.finalizers(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.finalizers(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.finalizers(newLoadBalancer2), containsInAnyOrder("service.kubernetes.io/load-balancer-cleanup"));
        assertThat(ListenersUtils.finalizers(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.finalizers(newTls), is(nullValue()));
        assertThat(ListenersUtils.finalizers(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.finalizers(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testExternalTrafficPolicy() {
        assertThat(ListenersUtils.externalTrafficPolicy(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.externalTrafficPolicy(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.externalTrafficPolicy(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.externalTrafficPolicy(newLoadBalancer2), is(ExternalTrafficPolicy.LOCAL));
        assertThat(ListenersUtils.externalTrafficPolicy(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.externalTrafficPolicy(newTls), is(nullValue()));
        assertThat(ListenersUtils.externalTrafficPolicy(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.externalTrafficPolicy(newNodePort2), is(ExternalTrafficPolicy.CLUSTER));
        assertThat(ListenersUtils.externalTrafficPolicy(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testIpFamilyPolicy() {
        assertThat(ListenersUtils.ipFamilyPolicy(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.ipFamilyPolicy(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.ipFamilyPolicy(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.ipFamilyPolicy(newLoadBalancer2), is(IpFamilyPolicy.REQUIRE_DUAL_STACK));
        assertThat(ListenersUtils.ipFamilyPolicy(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.ipFamilyPolicy(newTls), is(nullValue()));
        assertThat(ListenersUtils.ipFamilyPolicy(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.ipFamilyPolicy(newNodePort2), is(IpFamilyPolicy.REQUIRE_DUAL_STACK));
        assertThat(ListenersUtils.ipFamilyPolicy(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testIpFamilies() {
        assertThat(ListenersUtils.ipFamilies(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.ipFamilies(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.ipFamilies(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.ipFamilies(newLoadBalancer2), contains(IpFamily.IPV6, IpFamily.IPV4));
        assertThat(ListenersUtils.ipFamilies(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.ipFamilies(newTls), is(nullValue()));
        assertThat(ListenersUtils.ipFamilies(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.ipFamilies(newNodePort2), contains(IpFamily.IPV6, IpFamily.IPV4));
        assertThat(ListenersUtils.ipFamilies(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testPreferredNodeAddressType() {
        assertThat(ListenersUtils.preferredNodeAddressType(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(newLoadBalancer2), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(newTls), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.preferredNodeAddressType(newNodePort2), is(NodeAddressType.INTERNAL_DNS));
        assertThat(ListenersUtils.preferredNodeAddressType(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testIngressClass() {
        assertThat(ListenersUtils.controllerClass(newLoadBalancer), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(oldExternal), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(newIngress), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(newClusterIP), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(newIngress2), is("my-ingress"));
        assertThat(ListenersUtils.controllerClass(oldPlain), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(newTls), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(newNodePort), is(nullValue()));
        assertThat(ListenersUtils.controllerClass(newNodePort3), is(nullValue()));
    }

    @ParallelTest
    public void testServiceType() {
        assertThat(ListenersUtils.serviceType(oldPlain), is("ClusterIP"));
        assertThat(ListenersUtils.serviceType(newTls), is("ClusterIP"));
        assertThat(ListenersUtils.serviceType(oldExternal), is("ClusterIP"));
        assertThat(ListenersUtils.serviceType(newLoadBalancer), is("LoadBalancer"));
        assertThat(ListenersUtils.serviceType(newIngress), is("ClusterIP"));
        assertThat(ListenersUtils.serviceType(newClusterIP), is("ClusterIP"));
        assertThat(ListenersUtils.serviceType(newNodePort), is("NodePort"));
        assertThat(ListenersUtils.serviceType(newRoute), is("ClusterIP"));
    }

    @ParallelTest
    public void testGetExternalAdvertisedUrlWithOverrides() {
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig0 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig0.setBroker(0);
        nodePortListenerBrokerConfig0.setAdvertisedHost("my-host-0.cz");
        nodePortListenerBrokerConfig0.setAdvertisedPort(10000);

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(true)
                .withNewConfiguration()
                    .withBrokers(nodePortListenerBrokerConfig0)
                .endConfiguration()
                .build();

        assertThat(ListenersUtils.advertisedHostnameFromOverrideOrParameter(listener, 0, "some-host.com"), is("my-host-0.cz"));
        assertThat(ListenersUtils.advertisedHostnameFromOverrideOrParameter(listener, 0, ""), is("my-host-0.cz"));
        assertThat(ListenersUtils.advertisedHostnameFromOverrideOrParameter(listener, 1, "some-host.com"), is("some-host.com"));
        assertThat(ListenersUtils.advertisedHostnameFromOverrideOrParameter(listener, 1, ""), is(""));

        assertThat(ListenersUtils.advertisedPortFromOverrideOrParameter(listener, 0, 12345), is("10000"));
        assertThat(ListenersUtils.advertisedPortFromOverrideOrParameter(listener, 0, 12345), is("10000"));
        assertThat(ListenersUtils.advertisedPortFromOverrideOrParameter(listener, 1, 12345), is("12345"));
        assertThat(ListenersUtils.advertisedPortFromOverrideOrParameter(listener, 1, 12345), is("12345"));
    }

    @ParallelTest
    public void testGetExternalAdvertisedUrlWithoutOverrides() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(true)
                .build();

        assertThat(ListenersUtils.advertisedHostnameFromOverrideOrParameter(listener, 0, "some-host.com"), is("some-host.com"));
        assertThat(ListenersUtils.advertisedHostnameFromOverrideOrParameter(listener, 0, ""), is(""));

        assertThat(ListenersUtils.advertisedPortFromOverrideOrParameter(listener, 0, 12345), is("12345"));
        assertThat(ListenersUtils.advertisedPortFromOverrideOrParameter(listener, 0, 12345), is("12345"));
    }
}

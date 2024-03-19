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
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBootstrapBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class ListenersValidatorTest {
    private final static Set<NodeRef> THREE_NODES = Set.of(
            new NodeRef("name-kafka-0", 0, "kafka", false, true),
            new NodeRef("name-kafka-1", 1, "kafka", false, true),
            new NodeRef("name-kafka-2", 2, "kafka", false, true));
    private final static Set<NodeRef> TWO_NODES = Set.of(
            new NodeRef("name-kafka-0", 0, "kafka", false, true),
            new NodeRef("name-kafka-1", 1, "kafka", false, true));
    private final static Set<NodeRef> NODE_POOL_NODES = Set.of(
            new NodeRef("foo-kafka-1000", 1000, "kafka", true, true),
            new NodeRef("bar-kafka-2000", 2000, "kafka", false, true),
            new NodeRef("bar-kafka-2001", 2001, "kafka", false, true));

    @ParallelTest
    public void testValidateListeners() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9901)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1, listener2);
        ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners);
    }

    @ParallelTest
    public void testValidateThrowsException() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1, listener2);

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), containsString("every listener needs to have a unique port number"));
    }

    @ParallelTest
    public void testValidateDuplicatePorts() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9901)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener3")
                .withPort(9901)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1, listener2, listener3);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("every listener needs to have a unique port number"));
    }

    @ParallelTest
    public void testValidateForbiddenPortByRange() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9000)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("port 9000 is forbidden and cannot be used"));
    }

    @ParallelTest
    public void testValidateForbiddenPortByException() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9404)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("port 9404 is forbidden and cannot be used"));
    }

    @ParallelTest
    public void testValidateDuplicateNames() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9901)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9902)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1, listener2, listener3);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("every listener needs to have a unique name"));
    }

    @ParallelTest
    public void testInvalidNames() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener 1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("LISTENER2")
                .withPort(9901)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener12345678901234567890")
                .withPort(9902)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1, listener2, listener3);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("listener names [listener 1, LISTENER2, listener12345678901234567890] are invalid and do not match the pattern ^[a-z0-9]{1,11}$"));
    }

    @ParallelTest
    public void testTlsAuthOnNonTlsListener() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("listener plain cannot use tls type authentication with disabled TLS encryption"));
    }

    @ParallelTest
    public void testTlsCustomCertOnNonTlsListener() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("cert")
                        .withKey("key")
                        .withSecretName("secretName")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);
        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder("listener plain cannot configure custom TLS certificate with disabled TLS encryption"));
    }

    @ParallelTest
    public void testInternalListener() {
        String name = "plain";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withNewConfiguration()
                    .withControllerClass("my-ingress")
                    .withUseServiceDnsDomain(true)
                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                    .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                    .withLoadBalancerSourceRanges(List.of("10.0.0.0/8", "130.211.204.1/32"))
                    .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                    .withNewBootstrap()
                        .withAlternativeNames(List.of("my-name-1", "my-name-2"))
                        .withLoadBalancerIP("130.211.204.1")
                        .withNodePort(32189)
                        .withHost("my-host")
                        .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                        .withLabels(Collections.singletonMap("label", "label-value"))
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .withLabels(Collections.singletonMap("label", "label-value"))
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .withLabels(Collections.singletonMap("label", "label-value"))
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " cannot configure class because it is not an Ingress or LoadBalancer based listener",
                "listener " + name + " cannot configure externalTrafficPolicy because it is not LoadBalancer or NodePort based listener",
                "listener " + name + " cannot configure loadBalancerSourceRanges because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure finalizers because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure preferredAddressType because it is not NodePort based listener",
                "listener " + name + " cannot configure bootstrap.host because it is not Route or Ingress based listener",
                "listener " + name + " cannot configure bootstrap.loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure bootstrap.nodePort because it is not NodePort based listener",
                "listener " + name + " cannot configure bootstrap.annotations because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener",
                "listener " + name + " cannot configure bootstrap.labels because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener",
                "listener " + name + " cannot configure brokers[].host because it is not Route or Ingress based listener",
                "listener " + name + " cannot configure brokers[].loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure brokers[].nodePort because it is not NodePort based listener",
                "listener " + name + " cannot configure brokers[].annotations because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener",
                "listener " + name + " cannot configure brokers[].labels because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener",
                "listener " + name + " cannot configure ipFamilyPolicy because it is internal listener",
                "listener " + name + " cannot configure ipFamilies because it is internal listener"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testLoadBalancerListener() {
        String name = "lb";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.LOADBALANCER)
                .withNewConfiguration()
                    .withControllerClass("my-ingress")
                    .withUseServiceDnsDomain(true)
                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                    .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                    .withLoadBalancerSourceRanges(List.of("10.0.0.0/8", "130.211.204.1/32"))
                    .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                    .withNewBootstrap()
                        .withAlternativeNames(List.of("my-name-1", "my-name-2"))
                        .withLoadBalancerIP("130.211.204.1")
                        .withNodePort(32189)
                        .withHost("my-host")
                        .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " cannot configure useServiceDnsDomain because it is not internal or cluster-ip listener",
                "listener " + name + " cannot configure preferredAddressType because it is not NodePort based listener",
                "listener " + name + " cannot configure bootstrap.host because it is not Route or Ingress based listener",
                "listener " + name + " cannot configure brokers[].host because it is not Route or Ingress based listener"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testLoadBalancerListenerWithNodePort() {
        String name = "lb";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.LOADBALANCER)
                .withNewConfiguration()
                .withNewBootstrap()
                .withNodePort(32189)
                .endBootstrap()
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners);
    }

    @ParallelTest
    public void testNodePortListener() {
        String name = "nodeport";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.NODEPORT)
                .withNewConfiguration()
                    .withControllerClass("my-ingress")
                    .withUseServiceDnsDomain(true)
                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                    .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                    .withLoadBalancerSourceRanges(List.of("10.0.0.0/8", "130.211.204.1/32"))
                    .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                    .withNewBootstrap()
                        .withAlternativeNames(List.of("my-name-1", "my-name-2"))
                        .withLoadBalancerIP("130.211.204.1")
                        .withNodePort(32189)
                        .withHost("my-host")
                        .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " cannot configure class because it is not an Ingress or LoadBalancer based listener",
                "listener " + name + " cannot configure useServiceDnsDomain because it is not internal or cluster-ip listener",
                "listener " + name + " cannot configure loadBalancerSourceRanges because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure finalizers because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure bootstrap.host because it is not Route or Ingress based listener",
                "listener " + name + " cannot configure bootstrap.loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure brokers[].host because it is not Route or Ingress based listener",
                "listener " + name + " cannot configure brokers[].loadBalancerIP because it is not LoadBalancer based listener"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testRouteListenerWithoutTls() {
        String name = "route";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.ROUTE)
                .withTls(false)
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " is Route or Ingress type listener and requires enabled TLS encryption"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testRouteListener() {
        String name = "route";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewConfiguration()
                    .withControllerClass("my-ingress")
                    .withUseServiceDnsDomain(true)
                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                    .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                    .withLoadBalancerSourceRanges(List.of("10.0.0.0/8", "130.211.204.1/32"))
                    .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                    .withNewBootstrap()
                        .withAlternativeNames(List.of("my-name-1", "my-name-2"))
                        .withLoadBalancerIP("130.211.204.1")
                        .withNodePort(32189)
                        .withHost("my-host")
                        .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                        .withLabels(Collections.singletonMap("label", "label-value"))
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .withLabels(Collections.singletonMap("label", "label-value"))
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .withLabels(Collections.singletonMap("label", "label-value"))
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " cannot configure class because it is not an Ingress or LoadBalancer based listener",
                "listener " + name + " cannot configure useServiceDnsDomain because it is not internal or cluster-ip listener",
                "listener " + name + " cannot configure externalTrafficPolicy because it is not LoadBalancer or NodePort based listener",
                "listener " + name + " cannot configure loadBalancerSourceRanges because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure finalizers because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure preferredAddressType because it is not NodePort based listener",
                "listener " + name + " cannot configure bootstrap.loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure bootstrap.nodePort because it is not NodePort based listener",
                "listener " + name + " cannot configure brokers[].loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure brokers[].nodePort because it is not NodePort based listener"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testIngressListenerWithoutTls() {
        String name = "ingress";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.INGRESS)
                .withTls(false)
                .withNewConfiguration()
                    .withNewBootstrap()
                        .withHost("my-host")
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withHost("my-host")
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withHost("my-host")
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " is Route or Ingress type listener and requires enabled TLS encryption"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testIngressListener() {
        String name = "ingress";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .withNewConfiguration()
                    .withControllerClass("my-ingress")
                    .withUseServiceDnsDomain(true)
                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                    .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                    .withLoadBalancerSourceRanges(List.of("10.0.0.0/8", "130.211.204.1/32"))
                    .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                    .withNewBootstrap()
                        .withAlternativeNames(List.of("my-name-1", "my-name-2"))
                        .withLoadBalancerIP("130.211.204.1")
                        .withNodePort(32189)
                        .withHost("my-host")
                        .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("advertised-host")
                                    .withAdvertisedPort(9092)
                                    .withLoadBalancerIP("130.211.204.1")
                                    .withNodePort(32189)
                                    .withHost("my-host")
                                    .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " cannot configure useServiceDnsDomain because it is not internal or cluster-ip listener",
                "listener " + name + " cannot configure externalTrafficPolicy because it is not LoadBalancer or NodePort based listener",
                "listener " + name + " cannot configure loadBalancerSourceRanges because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure finalizers because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure preferredAddressType because it is not NodePort based listener",
                "listener " + name + " cannot configure bootstrap.loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure bootstrap.nodePort because it is not NodePort based listener",
                "listener " + name + " cannot configure brokers[].loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure brokers[].nodePort because it is not NodePort based listener"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testIngressListenerHostNames() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("ingress")
                .withPort(9092)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .build();

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a configuration with host names which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBrokers((List<GenericKafkaListenerConfigurationBroker>) null)
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a bootstrap host name which is required for Ingress based listeners",
                "listener ingress is missing a broker configuration with host names which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                                .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .build())
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a bootstrap host name which is required for Ingress based listeners",
                "listener ingress is missing a broker host name for broker with ID 0 which is required for Ingress based listeners",
                "listener ingress is missing a broker host name for broker with ID 1 which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withHost("bootstrap-host")
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withHost("host-1")
                                .build())
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a broker host name for broker with ID 0 which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withHost("bootstrap-host")
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withHost("host-0")
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withHost("host-1")
                                .build())
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, List.of(listener)), hasSize(0));
    }

    @ParallelTest
    public void testIngressListenerHostNamesInNodePools() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("ingress")
                .withPort(9092)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .build();

        assertThat(ListenersValidator.validateAndGetErrorMessages(NODE_POOL_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a configuration with host names which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBrokers((List<GenericKafkaListenerConfigurationBroker>) null)
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(NODE_POOL_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a bootstrap host name which is required for Ingress based listeners",
                "listener ingress is missing a broker configuration with host names which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                                .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1000)
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(2000)
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(2001)
                                .build())
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(NODE_POOL_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a bootstrap host name which is required for Ingress based listeners",
                "listener ingress is missing a broker host name for broker with ID 1000 which is required for Ingress based listeners",
                "listener ingress is missing a broker host name for broker with ID 2000 which is required for Ingress based listeners",
                "listener ingress is missing a broker host name for broker with ID 2001 which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withHost("bootstrap-host")
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1000)
                                .withHost("host-1000")
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(2001)
                                .withHost("host-2001")
                                .build())
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(NODE_POOL_NODES, List.of(listener)), containsInAnyOrder("listener ingress is missing a broker host name for broker with ID 2000 which is required for Ingress based listeners"));

        listener.setConfiguration(new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withHost("bootstrap-host")
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1000)
                                .withHost("host-1000")
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(2000)
                                .withHost("host-2000")
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(2001)
                                .withHost("host-2001")
                                .build())
                .build());

        assertThat(ListenersValidator.validateAndGetErrorMessages(NODE_POOL_NODES, List.of(listener)), hasSize(0));
    }

    @ParallelTest
    public void testClusterIPListenerWithoutTls() {
        String name = "clusterip";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(false)
                .withNewConfiguration()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("my-host")
                                .withAdvertisedPort(12345)
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("my-host")
                                .withAdvertisedPort(12346)
                                .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, listeners), hasSize(0));
    }

    @ParallelTest
    public void testClusterIPListener() {
        String name = "clusterip";

        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName(name)
                .withPort(9092)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(true)
                .withNewConfiguration()
                .withControllerClass("my-ingress")
                .withUseServiceDnsDomain(true)
                .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                .withIpFamilyPolicy(IpFamilyPolicy.REQUIRE_DUAL_STACK)
                .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
                .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                .withLoadBalancerSourceRanges(List.of("10.0.0.0/8", "130.211.204.1/32"))
                .withFinalizers(List.of("service.kubernetes.io/load-balancer-cleanup"))
                .withNewBootstrap()
                .withAlternativeNames(List.of("my-name-1", "my-name-2"))
                .withLoadBalancerIP("130.211.204.1")
                .withNodePort(32189)
                .withHost("my-host")
                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                .endBootstrap()
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(0)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withLoadBalancerIP("130.211.204.1")
                                .withNodePort(32189)
                                .withHost("my-host")
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .build(),
                        new GenericKafkaListenerConfigurationBrokerBuilder()
                                .withBroker(1)
                                .withAdvertisedHost("advertised-host")
                                .withAdvertisedPort(9092)
                                .withLoadBalancerIP("130.211.204.1")
                                .withNodePort(32189)
                                .withHost("my-host")
                                .withAnnotations(Collections.singletonMap("dns-anno", "dns-value"))
                                .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        List<String> expectedErrors = List.of(
                "listener " + name + " cannot configure externalTrafficPolicy because it is not LoadBalancer or NodePort based listener",
                "listener " + name + " cannot configure loadBalancerSourceRanges because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure finalizers because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure preferredAddressType because it is not NodePort based listener",
                "listener " + name + " cannot configure bootstrap.loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure bootstrap.nodePort because it is not NodePort based listener",
                "listener " + name + " cannot configure brokers[].loadBalancerIP because it is not LoadBalancer based listener",
                "listener " + name + " cannot configure brokers[].nodePort because it is not NodePort based listener",
                "listener " + name + " cannot configure class because it is not an Ingress or LoadBalancer based listener",
                "listener " + name + " cannot configure bootstrap.host because it is not Route or Ingress based listener",
                "listener " + name + " cannot configure brokers[].host because it is not Route or Ingress based listener"
        );

        assertThat(ListenersValidator.validateAndGetErrorMessages(TWO_NODES, listeners), containsInAnyOrder(expectedErrors.toArray()));
    }

    @ParallelTest
    public void testValidateOauth() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(new KafkaListenerAuthenticationOAuthBuilder().build())
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: Introspection endpoint URI or JWKS endpoint URI has to be specified"),
                containsString("listener listener1: Valid Issuer URI has to be specified or 'checkIssuer' set to 'false'")));
    }

    @ParallelTest
    public void testValidateBrokerCertChainAndKey() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("")
                        .withKey("")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1);

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), allOf(
                containsString("listener 'listener1' cannot have an empty secret name in the brokerCertChainAndKey"),
                containsString("listener 'listener1' cannot have an empty key in the brokerCertChainAndKey"),
                containsString("listener 'listener1' cannot have an empty certificate in the brokerCertChainAndKey")));
    }

    @ParallelTest
    public void testMinimalConfiguration() {
        GenericKafkaListener internal = new GenericKafkaListenerBuilder()
                .withName("internal")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener route = new GenericKafkaListenerBuilder()
                .withName("route")
                .withPort(9093)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .build();

        GenericKafkaListener lb = new GenericKafkaListenerBuilder()
                .withName("lb")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .build();

        GenericKafkaListener np = new GenericKafkaListenerBuilder()
                .withName("np")
                .withPort(9095)
                .withType(KafkaListenerType.NODEPORT)
                .build();

        GenericKafkaListener inq = new GenericKafkaListenerBuilder()
                .withName("inq")
                .withPort(9096)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBootstrap()
                        .withHost("my-host")
                    .endBootstrap()
                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(0)
                                    .withHost("my-host")
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(1)
                                    .withHost("my-host")
                                    .build(),
                            new GenericKafkaListenerConfigurationBrokerBuilder()
                                    .withBroker(2)
                                    .withHost("my-host")
                                    .build())
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(internal, route, lb, np, inq);

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), empty());
    }

    @ParallelTest
    public void testValidateOauthPlain() {
        KafkaListenerAuthenticationOAuthBuilder authBuilder = new KafkaListenerAuthenticationOAuthBuilder()
                .withEnableOauthBearer(false);

        GenericKafkaListenerBuilder listenerBuilder = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(authBuilder.build());

        GenericKafkaListener listener = listenerBuilder.withAuth(authBuilder.build())
                .build();

        List<GenericKafkaListener> listeners = List.of(listener);

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: At least one of 'enablePlain', 'enableOauthBearer' has to be set to 'true'")));

        // enable plain with neither introspectionEndpointUri nor jwksEndpointUri set
        authBuilder.withEnablePlain(true);
        listener = listenerBuilder.withAuth(authBuilder.build())
                .build();
        List<GenericKafkaListener> listeners2 = List.of(listener);

        exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners2));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: Introspection endpoint URI or JWKS endpoint URI has to be specified")));

        // enable plain with jwksEndpointUri set but tokenEndpointUri not set
        authBuilder.withJwksEndpointUri("http://localhost:8080/jwks").withCheckIssuer(false);
        listener = listenerBuilder.withAuth(authBuilder.build())
                .build();
        List<GenericKafkaListener> listeners3 = List.of(listener);

        assertDoesNotThrow(() -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners3));
    }

    @ParallelTest
    public void testValidateAudienceOauth() {
        KafkaListenerAuthenticationOAuthBuilder authBuilder = new KafkaListenerAuthenticationOAuthBuilder()
                .withCheckAudience(true);

        GenericKafkaListenerBuilder listenerBuilder = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(authBuilder.build());

        GenericKafkaListener listener = listenerBuilder.withAuth(authBuilder.build())
                .build();

        List<GenericKafkaListener> listeners = List.of(listener);

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: 'clientId' has to be configured when 'checkAudience' is 'true'")));

        // set clientId
        authBuilder.withClientId("kafka");

        listener = listenerBuilder.withAuth(authBuilder.build())
                .build();
        List<GenericKafkaListener> listeners2 = List.of(listener);

        exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners2));
        assertThat(exception.getMessage(), allOf(
                not(containsString("listener listener1: 'clientId' has to be configured when 'checkAudience' is 'true'"))));
    }

    @ParallelTest
    public void testValidateCustomClaimCheckOauth() {
        KafkaListenerAuthenticationOAuthBuilder authBuilder = new KafkaListenerAuthenticationOAuthBuilder()
                .withCustomClaimCheck("invalid");

        GenericKafkaListenerBuilder listenerBuilder = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(authBuilder.build());

        GenericKafkaListener listener = listenerBuilder.withAuth(authBuilder.build())
                .build();

        List<GenericKafkaListener> listeners = List.of(listener);

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: 'customClaimCheck' value not a valid JsonPath filter query - Failed to parse filter query: \"invalid\"")));

        // set valid JsonPath query
        authBuilder.withCustomClaimCheck("@.valid == 'value'");

        listener = listenerBuilder.withAuth(authBuilder.build())
                .build();
        List<GenericKafkaListener> listeners2 = List.of(listener);

        exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners2));
        assertThat(exception.getMessage(), allOf(
                not(containsString("listener listener1: 'customClaimCheck' value not a valid JsonPath filter query - Failed to parse query: \"invalid\" at position: 0"))));
    }

    @ParallelTest
    public void testValidateTimeoutsOauth() {
        KafkaListenerAuthenticationOAuthBuilder authBuilder = new KafkaListenerAuthenticationOAuthBuilder()
                .withConnectTimeoutSeconds(0);

        GenericKafkaListenerBuilder listenerBuilder = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(authBuilder.build());


        List<GenericKafkaListener> listeners = List.of(listenerBuilder.withAuth(authBuilder.build()).build());

        Exception exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: 'connectTimeoutSeconds' needs to be a positive integer (set to: 0)")));

        // set valid connectTimeoutSeconds
        authBuilder.withConnectTimeoutSeconds(1);

        List<GenericKafkaListener> listeners2 = List.of(listenerBuilder.withAuth(authBuilder.build()).build());

        exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners2));
        assertThat(exception.getMessage(), allOf(
                not(containsString("listener listener1: 'connectTimeoutSeconds' needs to be a positive integer"))));

        //
        // Repeat for readTimeoutSeconds
        //
        authBuilder.withConnectTimeoutSeconds(null)
                .withReadTimeoutSeconds(0);

        List<GenericKafkaListener> listeners3 = List.of(listenerBuilder.withAuth(authBuilder.build()).build());

        exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners3));
        assertThat(exception.getMessage(), allOf(
                containsString("listener listener1: 'readTimeoutSeconds' needs to be a positive integer (set to: 0)")));

        // set valid readTimeoutSeconds
        authBuilder.withReadTimeoutSeconds(1);

        List<GenericKafkaListener> listeners4 = List.of(listenerBuilder.withAuth(authBuilder.build()).build());

        exception = assertThrows(InvalidResourceException.class, () -> ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, listeners4));
        assertThat(exception.getMessage(), allOf(
                not(containsString("listener listener1: 'readTimeoutSeconds' needs to be a positive integer"))));

    }

    @ParallelTest
    public void testValidateCreateBootstrapService() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withNewConfiguration()
                .withCreateBootstrapService(false)
                .endConfiguration()
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9910)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withNewConfiguration()
                .withCreateBootstrapService(false)
                .endConfiguration()
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener3")
                .withPort(9920)
                .withType(KafkaListenerType.NODEPORT)
                .withNewConfiguration()
                .withCreateBootstrapService(false)
                .endConfiguration()
                .build();

        List<GenericKafkaListener> listeners = List.of(listener1, listener2, listener3);

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(
                "listener listener1 cannot configure createBootstrapService because it is not load balancer listener",
                "listener listener2 cannot configure createBootstrapService because it is not load balancer listener",
                "listener listener3 cannot configure createBootstrapService because it is not load balancer listener"));
    }
    
    @ParallelTest
    public void testValidateBootstrapExternalIPsListenener() {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .withNewConfiguration()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withExternalIPs(Arrays.asList("10.0.0.1"))
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                        .withBroker(0)
                        .withAdvertisedHost("advertised-host")
                        .withExternalIPs(Arrays.asList("10.0.0.2"))
                        .build())
                .endConfiguration()
                .build();
        
        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewConfiguration()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withExternalIPs(Arrays.asList("10.0.0.3"))
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                        .withBroker(1)
                        .withAdvertisedHost("advertised-host")
                        .withExternalIPs(Arrays.asList("10.0.0.4"))
                        .build())
                .endConfiguration()
                .build();
        
        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener3")
                .withPort(9920)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(true)
                .withNewConfiguration()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withExternalIPs(Arrays.asList("10.0.0.5"))
                        .build())
                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                        .withBroker(1)
                        .withAdvertisedHost("advertised-host")
                        .withExternalIPs(Arrays.asList("10.0.0.6"))
                        .build())
                .endConfiguration()
                .build();
        List<GenericKafkaListener> listeners = List.of(listener1, listener2, listener3);

        assertThat(ListenersValidator.validateAndGetErrorMessages(THREE_NODES, listeners), containsInAnyOrder(
                "listener listener1 cannot configure bootstrap.externalIPs because it is not NodePort based listener",
                "listener listener2 cannot configure bootstrap.externalIPs because it is not NodePort based listener",
                "listener listener1 cannot configure brokers[].externalIPs because it is not NodePort based listener",
                "listener listener2 cannot configure brokers[].externalIPs because it is not NodePort based listener"));
    }
}

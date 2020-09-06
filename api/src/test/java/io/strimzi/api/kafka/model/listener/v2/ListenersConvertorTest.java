/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener.v2;

import io.fabric8.kubernetes.api.model.LabelSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.strimzi.api.kafka.model.listener.IngressListenerBrokerConfigurationBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternal;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalIngressBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalLoadBalancerBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalRouteBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlain;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlainBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerTlsBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverrideBuilder;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverrideBuilder;
import io.strimzi.api.kafka.model.listener.RouteListenerBrokerOverrideBuilder;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ListenersConvertorTest {
    private final static NetworkPolicyPeer NETWORK_POLICY_PEER_1 = new NetworkPolicyPeerBuilder()
            .withNewPodSelector()
            .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key1").withValues("my-value1").build())
            .endPodSelector()
            .build();

    private final static NetworkPolicyPeer NETWORK_POLICY_PEER_2 = new NetworkPolicyPeerBuilder()
            .withNewNamespaceSelector()
            .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key2").withValues("my-value2").build())
            .endNamespaceSelector()
            .build();

    @Test
    public void testConvertToNewFormat() {
        KafkaListeners oldListeners = new KafkaListenersBuilder()
                .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationScramSha512())
                .endPlain()
                .withNewTls()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endTls()
                .withNewKafkaListenerExternalRoute()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endKafkaListenerExternalRoute()
                .build();

        List<GenericKafkaListener> newListners = ListenersConvertor.convertToNewFormat(oldListeners);

        assertThat(newListners, is(notNullValue()));
        assertThat(newListners.size(), is(3));

        GenericKafkaListener plain = newListners.stream().filter(list -> "plain".equals(list.getName())).findFirst().orElseThrow(() -> new RuntimeException());
        assertThat(plain.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));

        GenericKafkaListener tls = newListners.stream().filter(list -> "tls".equals(list.getName())).findFirst().orElseThrow(() -> new RuntimeException());
        assertThat(tls.getAuth().getType(), is(KafkaListenerAuthenticationTls.TYPE_TLS));

        GenericKafkaListener external = newListners.stream().filter(list -> "external".equals(list.getName())).findFirst().orElseThrow(() -> new RuntimeException());
        assertThat(external.getType(), is(KafkaListenerType.ROUTE));
        assertThat(external.getAuth().getType(), is(KafkaListenerAuthenticationTls.TYPE_TLS));
    }

    @Test
    public void testConvertToNewFormatSingle() {
        KafkaListeners oldListeners = new KafkaListenersBuilder()
                .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationScramSha512())
                .endPlain()
                .build();

        List<GenericKafkaListener> newListners = ListenersConvertor.convertToNewFormat(oldListeners);

        assertThat(newListners, is(notNullValue()));
        assertThat(newListners.size(), is(1));

        GenericKafkaListener plain = newListners.stream().filter(list -> "plain".equals(list.getName())).findFirst().orElseThrow(() -> new RuntimeException());
        assertThat(plain.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
    }

    @Test
    public void testConvertToNewFormatEmpty() {
        KafkaListeners oldListeners = new KafkaListenersBuilder()
                .build();

        List<GenericKafkaListener> newListners = ListenersConvertor.convertToNewFormat(oldListeners);

        assertThat(newListners, is(notNullValue()));
        assertThat(newListners.size(), is(0));
    }

    @Test
    public void testConvertToNewFormatNull() {
        List<GenericKafkaListener> newListners = ListenersConvertor.convertToNewFormat(null);

        assertThat(newListners, is(notNullValue()));
        assertThat(newListners.size(), is(0));
    }

    @Test
    public void testConvertSimplePlainListener() {
        KafkaListenerPlain oldPlain = new KafkaListenerPlainBuilder()
                .build();

        GenericKafkaListener newPlain = ListenersConvertor.convertPlainListener(oldPlain);

        assertThat(newPlain.getName(), is("plain"));
        assertThat(newPlain.getPort(), is(9092));
        assertThat(newPlain.getType(), is(KafkaListenerType.INTERNAL));
        assertThat(newPlain.getAuth(), is(nullValue()));
        assertThat(newPlain.isTls(), is(false));
        assertThat(newPlain.getNetworkPolicyPeers(), is(nullValue()));
        assertThat(newPlain.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertPlainListener() {
        KafkaListenerPlain oldPlain = new KafkaListenerPlainBuilder()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .withNetworkPolicyPeers(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2)
                .build();

        GenericKafkaListener newPlain = ListenersConvertor.convertPlainListener(oldPlain);

        assertThat(newPlain.getName(), is("plain"));
        assertThat(newPlain.getPort(), is(9092));
        assertThat(newPlain.getType(), is(KafkaListenerType.INTERNAL));
        assertThat(newPlain.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
        assertThat(newPlain.isTls(), is(false));
        assertThat(newPlain.getNetworkPolicyPeers().size(), is(2));
        assertThat(newPlain.getNetworkPolicyPeers(), hasItems(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2));
        assertThat(newPlain.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertSimpleTlsListener() {
        KafkaListenerTls oldTls = new KafkaListenerTlsBuilder()
                .build();

        GenericKafkaListener newTls = ListenersConvertor.convertTlsListener(oldTls);

        assertThat(newTls.getName(), is("tls"));
        assertThat(newTls.getPort(), is(9093));
        assertThat(newTls.getType(), is(KafkaListenerType.INTERNAL));
        assertThat(newTls.getAuth(), is(nullValue()));
        assertThat(newTls.isTls(), is(true));
        assertThat(newTls.getNetworkPolicyPeers(), is(nullValue()));
        assertThat(newTls.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertTlsListener() {
        KafkaListenerTls oldTls = new KafkaListenerTlsBuilder()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .withNetworkPolicyPeers(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("cert")
                        .withKey("key")
                        .withSecretName("secretName")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        GenericKafkaListener newTls = ListenersConvertor.convertTlsListener(oldTls);

        assertThat(newTls.getName(), is("tls"));
        assertThat(newTls.getPort(), is(9093));
        assertThat(newTls.getType(), is(KafkaListenerType.INTERNAL));
        assertThat(newTls.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
        assertThat(newTls.isTls(), is(true));
        assertThat(newTls.getNetworkPolicyPeers().size(), is(2));
        assertThat(newTls.getNetworkPolicyPeers(), hasItems(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2));
        assertThat(newTls.getConfiguration(), is(notNullValue()));
        assertThat(newTls.getConfiguration().getBrokerCertChainAndKey().getCertificate(), is("cert"));
        assertThat(newTls.getConfiguration().getBrokerCertChainAndKey().getKey(), is("key"));
        assertThat(newTls.getConfiguration().getBrokerCertChainAndKey().getSecretName(), is("secretName"));
    }

    @Test
    public void testConvertSimpleRouteListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalRouteBuilder()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.ROUTE));
        assertThat(newListener.getAuth(), is(nullValue()));
        assertThat(newListener.isTls(), is(true));
        assertThat(newListener.getNetworkPolicyPeers(), is(nullValue()));
        assertThat(newListener.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertRouteListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalRouteBuilder()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .withNetworkPolicyPeers(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("cert")
                        .withKey("key")
                        .withSecretName("secretName")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .withNewOverrides()
                    .withNewBootstrap()
                        .withHost("my-bootstrap-host")
                        .withAddress("my-bootstrap-dns")
                    .endBootstrap()
                    .withBrokers(
                            new RouteListenerBrokerOverrideBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("my-advertised-host-0")
                                    .withAdvertisedPort(1234)
                                    .withHost("my-host-0")
                                    .build(),
                            new RouteListenerBrokerOverrideBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("my-advertised-host-1")
                                    .withAdvertisedPort(1234)
                                    .withHost("my-host-1")
                                    .build()
                    )
                .endOverrides()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.ROUTE));
        assertThat(newListener.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
        assertThat(newListener.isTls(), is(true));
        assertThat(newListener.getNetworkPolicyPeers().size(), is(2));
        assertThat(newListener.getNetworkPolicyPeers(), hasItems(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2));
        assertThat(newListener.getConfiguration(), is(notNullValue()));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getCertificate(), is("cert"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getKey(), is("key"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getSecretName(), is("secretName"));
        assertThat(newListener.getConfiguration().getBootstrap().getHost(), is("my-bootstrap-host"));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().size(), is(1));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().get(0), is("my-bootstrap-dns"));
        assertThat(newListener.getConfiguration().getBrokers().size(), is(2));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getBroker(), is(0));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedHost(), is("my-advertised-host-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getHost(), is("my-host-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getBroker(), is(1));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedHost(), is("my-advertised-host-1"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getHost(), is("my-host-1"));
    }

    @Test
    public void testConvertSimpleLoadBalancerListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalLoadBalancerBuilder()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.LOADBALANCER));
        assertThat(newListener.getAuth(), is(nullValue()));
        assertThat(newListener.isTls(), is(true));
        assertThat(newListener.getNetworkPolicyPeers(), is(nullValue()));
        assertThat(newListener.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertLoadBalancerListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalLoadBalancerBuilder()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .withNetworkPolicyPeers(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2)
                .withTls(false)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("cert")
                        .withKey("key")
                        .withSecretName("secretName")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .withNewOverrides()
                    .withNewBootstrap()
                        .withLoadBalancerIP("64.23.234.148")
                        .withAddress("my-bootstrap-dns")
                        .withDnsAnnotations(Collections.singletonMap("anno", "my-dns"))
                    .endBootstrap()
                    .withBrokers(
                            new LoadBalancerListenerBrokerOverrideBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("my-advertised-host-0")
                                    .withAdvertisedPort(1234)
                                    .withLoadBalancerIP("64.23.234.149")
                                    .withDnsAnnotations(Collections.singletonMap("anno", "my-dns-0"))
                                    .build(),
                            new LoadBalancerListenerBrokerOverrideBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("my-advertised-host-1")
                                    .withAdvertisedPort(1234)
                                    .withLoadBalancerIP("64.23.234.150")
                                    .withDnsAnnotations(Collections.singletonMap("anno", "my-dns-1"))
                                    .build()
                    )
                .endOverrides()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.LOADBALANCER));
        assertThat(newListener.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
        assertThat(newListener.isTls(), is(false));
        assertThat(newListener.getNetworkPolicyPeers().size(), is(2));
        assertThat(newListener.getNetworkPolicyPeers(), hasItems(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2));
        assertThat(newListener.getConfiguration(), is(notNullValue()));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getCertificate(), is("cert"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getKey(), is("key"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getSecretName(), is("secretName"));
        assertThat(newListener.getConfiguration().getBootstrap().getLoadBalancerIP(), is("64.23.234.148"));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().size(), is(1));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().get(0), is("my-bootstrap-dns"));
        assertThat(newListener.getConfiguration().getBootstrap().getAnnotations().get("anno"), is("my-dns"));
        assertThat(newListener.getConfiguration().getBrokers().size(), is(2));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getBroker(), is(0));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedHost(), is("my-advertised-host-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getLoadBalancerIP(), is("64.23.234.149"));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAnnotations().get("anno"), is("my-dns-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getBroker(), is(1));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedHost(), is("my-advertised-host-1"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getLoadBalancerIP(), is("64.23.234.150"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAnnotations().get("anno"), is("my-dns-1"));
    }

    @Test
    public void testConvertSimpleNodePortListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalNodePortBuilder()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.NODEPORT));
        assertThat(newListener.getAuth(), is(nullValue()));
        assertThat(newListener.isTls(), is(true));
        assertThat(newListener.getNetworkPolicyPeers(), is(nullValue()));
        assertThat(newListener.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertNodePortListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalNodePortBuilder()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .withNetworkPolicyPeers(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2)
                .withTls(false)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("cert")
                        .withKey("key")
                        .withSecretName("secretName")
                    .endBrokerCertChainAndKey()
                    .withPreferredAddressType(NodeAddressType.EXTERNAL_IP)
                .endConfiguration()
                .withNewOverrides()
                    .withNewBootstrap()
                        .withNodePort(31000)
                        .withAddress("my-bootstrap-dns")
                        .withDnsAnnotations(Collections.singletonMap("anno", "my-dns"))
                    .endBootstrap()
                    .withBrokers(
                            new NodePortListenerBrokerOverrideBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("my-advertised-host-0")
                                    .withAdvertisedPort(1234)
                                    .withNodePort(32000)
                                    .withDnsAnnotations(Collections.singletonMap("anno", "my-dns-0"))
                                    .build(),
                            new NodePortListenerBrokerOverrideBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("my-advertised-host-1")
                                    .withAdvertisedPort(1234)
                                    .withNodePort(32001)
                                    .withDnsAnnotations(Collections.singletonMap("anno", "my-dns-1"))
                                    .build()
                    )
                .endOverrides()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.NODEPORT));
        assertThat(newListener.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
        assertThat(newListener.isTls(), is(false));
        assertThat(newListener.getNetworkPolicyPeers().size(), is(2));
        assertThat(newListener.getNetworkPolicyPeers(), hasItems(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2));
        assertThat(newListener.getConfiguration(), is(notNullValue()));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getCertificate(), is("cert"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getKey(), is("key"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getSecretName(), is("secretName"));
        assertThat(newListener.getConfiguration().getPreferredNodePortAddressType(), is(NodeAddressType.EXTERNAL_IP));
        assertThat(newListener.getConfiguration().getBootstrap().getNodePort(), is(31000));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().size(), is(1));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().get(0), is("my-bootstrap-dns"));
        assertThat(newListener.getConfiguration().getBootstrap().getAnnotations().get("anno"), is("my-dns"));
        assertThat(newListener.getConfiguration().getBrokers().size(), is(2));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getBroker(), is(0));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedHost(), is("my-advertised-host-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getNodePort(), is(32000));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAnnotations().get("anno"), is("my-dns-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getBroker(), is(1));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedHost(), is("my-advertised-host-1"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getNodePort(), is(32001));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAnnotations().get("anno"), is("my-dns-1"));
    }

    @Test
    public void testConvertSimpleIngressListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalIngressBuilder()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.INGRESS));
        assertThat(newListener.getAuth(), is(nullValue()));
        assertThat(newListener.isTls(), is(true));
        assertThat(newListener.getNetworkPolicyPeers(), is(nullValue()));
        assertThat(newListener.getConfiguration(), is(nullValue()));
    }

    @Test
    public void testConvertIngressListener() {
        KafkaListenerExternal oldListener = new KafkaListenerExternalIngressBuilder()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .withNetworkPolicyPeers(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2)
                .withIngressClass("haproxy")
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withCertificate("cert")
                        .withKey("key")
                        .withSecretName("secretName")
                    .endBrokerCertChainAndKey()
                    .withNewBootstrap()
                        .withHost("my-bootstrap-host")
                        .withAddress("my-bootstrap-dns")
                        .withDnsAnnotations(Collections.singletonMap("anno", "my-dns"))
                    .endBootstrap()
                    .withBrokers(
                            new IngressListenerBrokerConfigurationBuilder()
                                    .withBroker(0)
                                    .withAdvertisedHost("my-advertised-host-0")
                                    .withAdvertisedPort(1234)
                                    .withHost("my-host-0")
                                    .withDnsAnnotations(Collections.singletonMap("anno", "my-dns-0"))
                                    .build(),
                            new IngressListenerBrokerConfigurationBuilder()
                                    .withBroker(1)
                                    .withAdvertisedHost("my-advertised-host-1")
                                    .withAdvertisedPort(1234)
                                    .withHost("my-host-1")
                                    .withDnsAnnotations(Collections.singletonMap("anno", "my-dns-1"))
                                    .build()
                    )
                .endConfiguration()
                .build();

        GenericKafkaListener newListener = ListenersConvertor.convertExternalListener(oldListener);

        assertThat(newListener.getName(), is("external"));
        assertThat(newListener.getPort(), is(9094));
        assertThat(newListener.getType(), is(KafkaListenerType.INGRESS));
        assertThat(newListener.getAuth().getType(), is(KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512));
        assertThat(newListener.isTls(), is(true));
        assertThat(newListener.getNetworkPolicyPeers().size(), is(2));
        assertThat(newListener.getNetworkPolicyPeers(), hasItems(NETWORK_POLICY_PEER_1, NETWORK_POLICY_PEER_2));
        assertThat(newListener.getConfiguration(), is(notNullValue()));
        assertThat(newListener.getConfiguration().getIngressClass(), is("haproxy"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getCertificate(), is("cert"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getKey(), is("key"));
        assertThat(newListener.getConfiguration().getBrokerCertChainAndKey().getSecretName(), is("secretName"));
        assertThat(newListener.getConfiguration().getBootstrap().getHost(), is("my-bootstrap-host"));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().size(), is(1));
        assertThat(newListener.getConfiguration().getBootstrap().getAlternativeNames().get(0), is("my-bootstrap-dns"));
        assertThat(newListener.getConfiguration().getBootstrap().getAnnotations().get("anno"), is("my-dns"));
        assertThat(newListener.getConfiguration().getBrokers().size(), is(2));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getBroker(), is(0));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedHost(), is("my-advertised-host-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getHost(), is("my-host-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(0).getAnnotations().get("anno"), is("my-dns-0"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getBroker(), is(1));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedHost(), is("my-advertised-host-1"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAdvertisedPort(), is(1234));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getHost(), is("my-host-1"));
        assertThat(newListener.getConfiguration().getBrokers().get(1).getAnnotations().get("anno"), is("my-dns-1"));
    }
}

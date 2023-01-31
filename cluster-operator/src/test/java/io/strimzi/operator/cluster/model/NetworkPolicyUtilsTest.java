/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class NetworkPolicyUtilsTest {
    private final static String NAME = "my-np";
    private final static String NAMESPACE = "my-namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-name")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));

    @Test
    public void testNetworkPolicy() {
        List<NetworkPolicyIngressRule> rules = List.of(
                NetworkPolicyUtils.createIngressRule(1234, List.of(NetworkPolicyUtils.createPeer(Map.of("key", "peer1")))),
                NetworkPolicyUtils.createIngressRule(5678, List.of(NetworkPolicyUtils.createPeer(Map.of("key", "peer2"))))
        );

        NetworkPolicy np = NetworkPolicyUtils.createNetworkPolicy(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, rules);

        assertThat(np.getMetadata().getName(), is(NAME));
        assertThat(np.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(np.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(np.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(LABELS.strimziSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress(), is(rules));
    }

    @Test
    public void testCreateRule()   {
        NetworkPolicyIngressRule rule = NetworkPolicyUtils.createIngressRule(1234, List.of(NetworkPolicyUtils.createPeer(Map.of("key", "peer1"))));
        assertThat(rule.getPorts().size(), is(1));
        assertThat(rule.getPorts().get(0).getPort().getIntVal(), is(1234));
        assertThat(rule.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(rule.getFrom().size(), is(1));
        assertThat(rule.getFrom().get(0), is(NetworkPolicyUtils.createPeer(Map.of("key", "peer1"))));
    }

    @Test
    public void testCreatePeerWithPodLabelsAndNullNamespaceSelector()   {
        NetworkPolicyPeer peer = NetworkPolicyUtils.createPeer(Map.of("labelKey", "labelValue"), null);
        assertThat(peer.getNamespaceSelector(), is(nullValue()));
        assertThat(peer.getPodSelector().getMatchLabels(), is(Map.of("labelKey", "labelValue")));
    }

    @Test
    public void testCreatePeerWithPodLabelsAndEmptyNamespaceSelector()   {
        NetworkPolicyPeer peer = NetworkPolicyUtils.createPeer(Map.of("labelKey", "labelValue"), new LabelSelectorBuilder().withMatchLabels(Map.of()).build());
        assertThat(peer.getNamespaceSelector().getMatchLabels(), is(Map.of()));
        assertThat(peer.getPodSelector().getMatchLabels(), is(Map.of("labelKey", "labelValue")));
    }

    @Test
    public void testCreatePeerWithPodLabelsAndNamespaceSelector()   {
        NetworkPolicyPeer peer = NetworkPolicyUtils.createPeer(Map.of("labelKey", "labelValue"), new LabelSelectorBuilder().withMatchLabels(Map.of("nsLabelKey", "nsLabelValue")).build());
        assertThat(peer.getNamespaceSelector().getMatchLabels(), is(Map.of("nsLabelKey", "nsLabelValue")));
        assertThat(peer.getPodSelector().getMatchLabels(), is(Map.of("labelKey", "labelValue")));
    }

    @Test
    public void testCreatePeerWithPodLabels()   {
        NetworkPolicyPeer peer = NetworkPolicyUtils.createPeer(Map.of("labelKey", "labelValue"));
        assertThat(peer.getNamespaceSelector(), is(nullValue()));
        assertThat(peer.getPodSelector().getMatchLabels(), is(Map.of("labelKey", "labelValue")));
    }

    @Test
    public void testCreatePeerWithEmptyLabels()   {
        NetworkPolicyPeer peer = NetworkPolicyUtils.createPeer(Map.of());
        assertThat(peer.getNamespaceSelector(), is(nullValue()));
        assertThat(peer.getPodSelector().getMatchLabels(), is(Map.of()));
    }

    @Test
    public void testClusterOperatorNamespaceSelector()  {
        assertThat(NetworkPolicyUtils.clusterOperatorNamespaceSelector("my-ns", "my-ns", null), is(nullValue()));
        assertThat(NetworkPolicyUtils.clusterOperatorNamespaceSelector("my-ns", "my-operator-ns", null).getMatchLabels(), is(Map.of()));
        assertThat(NetworkPolicyUtils.clusterOperatorNamespaceSelector("my-ns", "my-operator-ns", Labels.EMPTY).getMatchLabels(), is(Map.of()));
        assertThat(NetworkPolicyUtils.clusterOperatorNamespaceSelector("my-ns", "my-operator-ns", Labels.fromMap(Map.of("labelKey", "labelValue"))).getMatchLabels(), is(Map.of("labelKey", "labelValue")));
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.strimzi.operator.common.model.Labels;

import java.util.List;
import java.util.Map;

/**
 * Shared methods for working with Network Policies
 */
public class NetworkPolicyUtils {
    /**
     * Creates a Network Policy
     *
     * @param name              Name of the Network Policy
     * @param namespace         Namespace of the Network Policy
     * @param labels            Labels of the Network Policy
     * @param ownerReference    OwnerReference of the Network Policy
     * @param ingressRules      List of Ingress rules
     *
     * @return  New Network Policy
     */
    public static NetworkPolicy createNetworkPolicy(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            List<NetworkPolicyIngressRule> ingressRules
    ) {
        return new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withNewPodSelector()
                        .withMatchLabels(labels.strimziSelectorLabels().toMap())
                    .endPodSelector()
                    .withIngress(ingressRules)
                .endSpec()
                .build();
    }

    /**
     * Creates Ingress Rule
     *
     * @param port      Port for which should this rule apply
     * @param peers     Peers who can connect to this port
     *
     * @return  New Ingress Rule
     */
    public static NetworkPolicyIngressRule createIngressRule(int port, List<NetworkPolicyPeer> peers) {
        return new NetworkPolicyIngressRuleBuilder()
                .addNewPort()
                    .withNewPort(port)
                    .withProtocol("TCP")
                .endPort()
                .withFrom(peers)
                .build();
    }

    /**
     * Create Network Policy Peer with namespace selector
     *
     * @param podSelector           Labels matching the Peer
     * @param namespaceSelector     Labels matching the Peer's namespace
     *
     * @return  New Network Policy Peer
     */
    public static NetworkPolicyPeer createPeer(Map<String, String> podSelector, LabelSelector namespaceSelector) {
        return new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(podSelector)
                .endPodSelector()
                .withNamespaceSelector(namespaceSelector)
                .build();
    }

    /**
     * Create Network Policy Peer
     *
     * @param matchLabels   Labels matching the Peer
     *
     * @return  New Network Policy Peer
     */
    public static NetworkPolicyPeer createPeer(Map<String, String> matchLabels) {
        return createPeer(matchLabels, null);
    }

    /**
     * Decides whether the Cluster Operator needs namespaceSelector to be configured in the network policies in order
     * to talk with the operands. This follows the following rules:
     *     - If it runs in the same namespace as the operand, do not set namespace selector
     *     - If it runs in a different namespace, but user provided selector labels, use the labels
     *     - If it runs in a different namespace, and user didn't provide selector labels, open it to COs in all namespaces
     *
     * @param operandNamespace          Namespace of the operand
     * @param operatorNamespace         Namespace of the Strimzi CO
     * @param operatorNamespaceLabels   Namespace labels provided by the user
     *
     * @return  Label selector for selecting the namespace from which the access should be allowed or null if not selector is set.
     */
    public static LabelSelector clusterOperatorNamespaceSelector(String operandNamespace, String operatorNamespace, Labels operatorNamespaceLabels)   {
        if (!operandNamespace.equals(operatorNamespace)) {
            // If CO and the operand do not run in the same namespace, we need to handle cross namespace access

            if (operatorNamespaceLabels != null && !operatorNamespaceLabels.toMap().isEmpty())    {
                // If user specified the namespace labels, we can use them to make the network policy as tight as possible
                return new LabelSelectorBuilder().withMatchLabels(operatorNamespaceLabels.toMap()).build();
            } else {
                // If no namespace labels were specified, we open the network policy to COs in all namespaces by returning empty map => selector which match everything
                return new LabelSelector();
            }
        } else {
            // They are in the dame namespace => we do not want to set any namespace selector and allow communication only within the same namespace
            return null;
        }
    }
}

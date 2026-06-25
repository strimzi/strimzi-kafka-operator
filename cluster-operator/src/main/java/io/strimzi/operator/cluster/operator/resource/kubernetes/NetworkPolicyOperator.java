/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;

import java.util.concurrent.Executor;
import java.util.regex.Pattern;

/**
 * Operator for managing network policies
 */
public class NetworkPolicyOperator extends AbstractNamespacedResourceOperator<KubernetesClient, NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> {
    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/metadata/managedFields" +
                    "|/metadata/creationTimestamp" +
                    "|/metadata/deletionTimestamp" +
                    "|/metadata/deletionGracePeriodSeconds" +
                    "|/metadata/resourceVersion" +
                    "|/metadata/generation" +
                    "|/metadata/uid" +
                    "|/spec/policyTypes" +
                    "|/status)$");

    /**
     * Constructs the Network Policy Operator
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client    Kubernetes client
     */
    public NetworkPolicyOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "NetworkPolicy");
    }

    @Override
    protected MixedOperation<NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> operation() {
        return client.network().networkPolicies();
    }

    /**
     * Gets the pattern for matching paths which can be ignored in the NetworkPolicy resource diff.
     *
     * @return  Returns the Pattern for matching paths which can be ignored in the resource diff
     */
    @Override
    protected Pattern ignorablePaths() {
        return IGNORABLE_PATHS;
    }
}

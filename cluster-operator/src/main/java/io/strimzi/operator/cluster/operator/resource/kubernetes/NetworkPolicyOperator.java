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
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.CompletionStage;
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
     * Keeps the finalizers set on the existing NetworkPolicy (for example by the EKS network policy
     * controller). The patch is computed against the desired resource, so a patch for a real change
     * would otherwise remove them.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the NetworkPolicy
     * @param name      Name of the NetworkPolicy
     * @param current   Current NetworkPolicy
     * @param desired   Desired NetworkPolicy
     *
     * @return  Future with reconciliation result
     */
    @Override
    protected CompletionStage<ReconcileResult<NetworkPolicy>> internalUpdate(Reconciliation reconciliation, String namespace, String name, NetworkPolicy current, NetworkPolicy desired) {
        if (current.getMetadata() != null && desired.getMetadata() != null) {
            desired.getMetadata().setFinalizers(current.getMetadata().getFinalizers());
        }

        return super.internalUpdate(reconciliation, namespace, name, current, desired);
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

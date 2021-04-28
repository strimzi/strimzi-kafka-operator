/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import java.util.regex.Pattern;

public class NetworkPolicyOperator extends AbstractResourceOperator<KubernetesClient, NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> {
    protected static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/metadata/managedFields" +
                    "|/spec/policyTypes" +
                    "|/status)$");

    public NetworkPolicyOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "NetworkPolicy");
    }

    @Override
    protected MixedOperation<NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> operation() {
        return client.network().networkPolicies();
    }

    /**
     * @return  Returns the Pattern for matching paths which can be ignored in the resource diff
     */
    @Override
    protected Pattern ignorablePaths() {
        return IGNORABLE_PATHS;
    }
}

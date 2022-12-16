/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

/**
 * Operator for managing Pod Disruption Budgets of API version v1beta1
 */
public class PodDisruptionBudgetV1Beta1Operator extends AbstractNamespacedResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> {
    /**
     * Constructs the PDB v1beta1 operator
     *
     * @param vertx     Vert.x instance
     * @param client    Kubernetes client
     */
    public PodDisruptionBudgetV1Beta1Operator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "v1beta1.PodDisruptionBudget");
    }

    @Override
    protected MixedOperation<PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> operation() {
        return client.policy().v1beta1().podDisruptionBudget();
    }
}

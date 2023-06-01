/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Operator for managing Pod Disruption Budgets
 */
public class PodDisruptionBudgetOperator extends AbstractNamespacedResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> {
    /**
     * Constructs the PDB operator
     *
     * @param client    Kubernetes client
     */
    public PodDisruptionBudgetOperator(KubernetesClient client) {
        super(client, "PodDisruptionBudget");
    }

    @Override
    protected MixedOperation<PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> operation() {
        return client.policy().v1().podDisruptionBudget();
    }
}

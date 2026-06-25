/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operator for managing Pod Disruption Budgets
 */
public class PodDisruptionBudgetOperator extends AbstractNamespacedResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> {
    /**
     * Constructs the PDB operator
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client    Kubernetes client
     */
    public PodDisruptionBudgetOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "PodDisruptionBudget");
    }

    @Override
    protected MixedOperation<PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> operation() {
        return client.policy().v1().podDisruptionBudget();
    }
}

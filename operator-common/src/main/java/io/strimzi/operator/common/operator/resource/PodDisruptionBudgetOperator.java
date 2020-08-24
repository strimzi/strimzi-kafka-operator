/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.policy.DoneablePodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class PodDisruptionBudgetOperator extends AbstractResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, DoneablePodDisruptionBudget, Resource<PodDisruptionBudget, DoneablePodDisruptionBudget>> {

    public PodDisruptionBudgetOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "PodDisruptionBudget");

    }
    @Override
    protected MixedOperation<PodDisruptionBudget, PodDisruptionBudgetList, DoneablePodDisruptionBudget, Resource<PodDisruptionBudget, DoneablePodDisruptionBudget>> operation() {
        return client.policy().podDisruptionBudget();
    }

    @Override
    protected Future<ReconcileResult<PodDisruptionBudget>> internalPatch(String namespace, String name, PodDisruptionBudget current, PodDisruptionBudget desired, boolean cascading) {
        Promise<ReconcileResult<PodDisruptionBudget>> promise = Promise.promise();
        internalDelete(namespace, name).onComplete(delRes -> {
            if (delRes.succeeded())    {
                internalCreate(namespace, name, desired).onComplete(createRes -> {
                    if (createRes.succeeded())  {
                        promise.complete(createRes.result());
                    } else {
                        promise.fail(createRes.cause());
                    }
                });
            } else {
                promise.fail(delRes.cause());
            }
        });

        return promise.future();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class ServiceAccountOperator extends AbstractResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> {
    private final boolean patching;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ServiceAccountOperator(Vertx vertx, KubernetesClient client) {
        this(vertx, client, false);
    }

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     * @param patching  Enables or disables patching of existing service accounts
     */
    public ServiceAccountOperator(Vertx vertx, KubernetesClient client, boolean patching) {
        super(vertx, client, "ServiceAccount");
        this.patching = patching;
    }

    @Override
    protected MixedOperation<ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> operation() {
        return client.serviceAccounts();
    }

    @Override
    protected Future<ReconcileResult<ServiceAccount>> internalPatch(Reconciliation reconciliation, String namespace, String name, ServiceAccount current, ServiceAccount desired) {
        if (patching)   {
            if (desired.getSecrets() == null || desired.getSecrets().isEmpty())    {
                desired.setSecrets(current.getSecrets());
            }
            return super.internalPatch(reconciliation, namespace, name, current, desired);
        } else {
            // Patching an SA causes new tokens to be created, which we should avoid
            reconciliationLogger.debug(reconciliation, "{} {} in namespace {} has not been patched: patching service accounts generates new tokens which should be avoided.", resourceKind, name, namespace);
            return Future.succeededFuture(ReconcileResult.noop(current));
        }
    }
}

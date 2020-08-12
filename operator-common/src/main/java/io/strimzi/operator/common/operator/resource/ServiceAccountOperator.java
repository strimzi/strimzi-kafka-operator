/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DoneableServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class ServiceAccountOperator extends AbstractResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount, DoneableServiceAccount>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ServiceAccountOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ServiceAccount");
    }

    @Override
    protected MixedOperation<ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount, DoneableServiceAccount>> operation() {
        return client.serviceAccounts();
    }

    @Override
    protected Future<ReconcileResult<ServiceAccount>> internalPatch(String namespace, String name, ServiceAccount current, ServiceAccount desired) {
        // Patching a SA causes new tokens to be created, which we should avoid
        log.debug("{} {} in namespace {} has not been patched: patching service accounts generates new tokens which should be avoided.", resourceKind, name, namespace);
        return Future.succeededFuture(ReconcileResult.noop(current));
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Map;

/**
 * Operator for managing Service Accounts
 */
public class ServiceAccountOperator extends AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> {
    /* test */ static final String OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF = "openshift.io/internal-registry-pull-secret-ref";

    /**
     * Constructor
     * @param vertx                 The Vertx instance
     * @param client                The Kubernetes client
     * @param useServerSideApply    Determines if Server Side Apply should be used
     */
    public ServiceAccountOperator(Vertx vertx, KubernetesClient client, boolean useServerSideApply) {
        super(vertx, client, "ServiceAccount", useServerSideApply);
    }

    @Override
    protected MixedOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> operation() {
        return client.serviceAccounts();
    }

    @Override
    protected Future<ReconcileResult<ServiceAccount>> internalUpdate(Reconciliation reconciliation, String namespace, String name, ServiceAccount current, ServiceAccount desired) {
        if (desired.getSecrets() == null || desired.getSecrets().isEmpty())    {
            desired.setSecrets(current.getSecrets());
        }

        if (desired.getImagePullSecrets() == null || desired.getImagePullSecrets().isEmpty())    {
            desired.setImagePullSecrets(current.getImagePullSecrets());
        }

        // OpenShift 4.16 will create an image pull secret, add it to the image pull secrets list, and attach an
        // annotation openshift.io/internal-registry-pull-secret-ref to the Service Account with the name of the image
        // pull secret. If Strimzi removes this annotation by patching the resource in the next reconciliation,
        // OpenShift will create another Secret, add it to the image pull secret list and add back the annotation.
        // This way, every reconciliation will for every Service Account make OCP generate new Secret and in a little
        // while, the namespace is full of these Secrets.
        //
        // The code below recovers the annotation from the original resource when patching it and reads it to the
        // desired Service Account to avoid removing the annotation and triggering OCP to create a new Secret. This is
        // not a great solution, but until we have Server Side Apply support, this is the best we can do.
        if (current.getMetadata() != null
                && current.getMetadata().getAnnotations() != null
                && current.getMetadata().getAnnotations().containsKey(OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF))  {
            if (desired.getMetadata().getAnnotations() != null
                    && !desired.getMetadata().getAnnotations().containsKey(OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF)) {
                desired.getMetadata().getAnnotations().put(OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF, current.getMetadata().getAnnotations().get(OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF));
            } else if (desired.getMetadata().getAnnotations() == null)    {
                desired.getMetadata().setAnnotations(Map.of(OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF, current.getMetadata().getAnnotations().get(OPENSHIFT_IO_INTERNAL_REGISTRY_PULL_SECRET_REF)));
            }
        }

        return super.internalUpdate(reconciliation, namespace, name, current, desired);
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.regex.Pattern;

/**
 * Operations for {@code PersistentVolumeClaim}s.
 */
public class PvcOperator extends AbstractNamespacedResourceOperator<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(PvcOperator.class);
    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/metadata/managedFields" +
                    "|/metadata/annotations/pv.kubernetes.io~1bind-completed" +
                    "|/metadata/finalizers" +
                    "|/metadata/creationTimestamp" +
                    "|/metadata/resourceVersion" +
                    "|/metadata/generation" +
                    "|/metadata/uid" +
                    "|/status)$");


    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public PvcOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "PersistentVolumeClaim");
    }

    @Override
    protected MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> operation() {
        return client.persistentVolumeClaims();
    }

    /**
     * @return  Returns the Pattern for matching paths which can be ignored in the resource diff
     */
    @Override
    protected Pattern ignorablePaths() {
        return IGNORABLE_PATHS;
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     *
     * PvcOperator needs to patch the volumeName field in spec which is immutable and which should contain the same value as the existing resource.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the pvc
     * @param name      Name of the pvc
     * @param current   Current pvc
     * @param desired   Desired pvc
     *
     * @return  Future with reconciliation result
     */
    @Override
    protected Future<ReconcileResult<PersistentVolumeClaim>> internalPatch(Reconciliation reconciliation, String namespace, String name, PersistentVolumeClaim current, PersistentVolumeClaim desired) {
        try {
            if (current.getSpec() != null && desired.getSpec() != null)   {
                revertImmutableChanges(current, desired);
            }

            return super.internalPatch(reconciliation, namespace, name, current, desired);
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Reverts the changes to immutable fields in PVCs spec section. The values for these fields in the current resource
     * are often not set by us but by Kubernetes alone (e.g. volume ID, default storage class etc.). So our Model
     * classes are nto aware of them and cannot set them properly. Therefore we are reverting these values here.
     *
     * @param current   Existing PVC
     * @param desired   Desired PVC
     */
    protected void revertImmutableChanges(PersistentVolumeClaim current, PersistentVolumeClaim desired)   {
        desired.getSpec().setVolumeName(current.getSpec().getVolumeName());
        desired.getSpec().setStorageClassName(current.getSpec().getStorageClassName());
        desired.getSpec().setAccessModes(current.getSpec().getAccessModes());
        desired.getSpec().setSelector(current.getSpec().getSelector());
        desired.getSpec().setDataSource(current.getSpec().getDataSource());
    }
}

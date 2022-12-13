/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.BuildRequest;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code BuildConfig}s.
 */
public class BuildConfigOperator extends AbstractNamespacedResourceOperator<OpenShiftClient, BuildConfig, BuildConfigList, BuildConfigResource<BuildConfig, Void, Build>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The OpenShift client
     */
    public BuildConfigOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "BuildConfig");
    }

    @Override
    protected MixedOperation<BuildConfig, BuildConfigList, BuildConfigResource<BuildConfig, Void, Build>> operation() {
        return client.buildConfigs();
    }

    @Override
    protected Future<ReconcileResult<BuildConfig>> internalPatch(Reconciliation reconciliation, String namespace, String name, BuildConfig current, BuildConfig desired) {
        desired.getSpec().setTriggers(current.getSpec().getTriggers());
        // Cascading needs to be set to false to make sure the Builds are not deleted during reconciliation
        return super.internalPatch(reconciliation, namespace, name, current, desired);
    }

    /**
     * Asynchronously deletes the resource in the given {@code namespace} with the given {@code name},
     * returning a Future which completes once the {@code delete} returns.
     *
     * This is n override for BuildConfigs because the {@code selfClosingWatch} used by {@code AbstractResourceoperator} does not work for them.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param cascading Defines whether the delete should be cascading or not (e.g. whether a STS deletion should delete pods etc.)
     *
     * @return A future which will be completed on the context thread once the resource has been deleted.
     */
    @Override
    protected Future<ReconcileResult<BuildConfig>> internalDelete(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        BuildConfigResource<BuildConfig, Void, Build> resourceOp = operation().inNamespace(namespace).withName(name);

        return resourceSupport.deleteAsync(resourceOp.withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L))
                .map(ReconcileResult.deleted());
    }

    /**
     * Starts an OpenShift Build from a BuildConfig
     *
     * @param namespace     Namespace where the BuildConfig exists
     * @param name          Name of the BuildConfig
     * @param buildRequest  BuildRequest requesting the build
     * @return              The Build which was created
     */
    public Future<Build> startBuild(String namespace, String name, BuildRequest buildRequest)   {
        return resourceSupport.executeBlocking(
            blockingFuture -> {
                try {
                    blockingFuture.complete(operation().inNamespace(namespace).withName(name).instantiate(buildRequest));
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            });
    }
}

/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code BuildConfig}s.
 */
public class BuildConfigOperator extends AbstractResourceOperator<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The OpenShift client
     */
    public BuildConfigOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "BuildConfig");
    }

    @Override
    protected MixedOperation<BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> operation() {
        return client.buildConfigs();
    }

    @Override
    protected Future<ReconcileResult<BuildConfig>> internalPatch(String namespace, String name, BuildConfig current, BuildConfig desired) {
        desired.getSpec().setTriggers(current.getSpec().getTriggers());
        return super.internalPatch(namespace, name, current, desired);
    }
}

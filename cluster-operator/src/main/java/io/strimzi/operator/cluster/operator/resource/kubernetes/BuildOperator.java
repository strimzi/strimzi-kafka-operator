/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildResource;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Build}s.
 */
public class BuildOperator extends AbstractNamespacedResourceOperator<OpenShiftClient, Build, BuildList, BuildResource> {
    /**
     * Constructor
     *
     * @param vertx The Vertx instance
     * @param client The OpenShift client
     */
    public BuildOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "Build");
    }

    @Override
    protected MixedOperation<Build, BuildList, BuildResource> operation() {
        return client.builds();
    }
}

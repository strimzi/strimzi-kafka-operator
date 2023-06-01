/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildResource;

/**
 * Operations for {@code Build}s.
 */
public class BuildOperator extends AbstractNamespacedResourceOperator<OpenShiftClient, Build, BuildList, BuildResource> {
    /**
     * Constructor
     *
     * @param client The OpenShift client
     */
    public BuildOperator(OpenShiftClient client) {
        super(client, "Build");
    }

    @Override
    protected MixedOperation<Build, BuildList, BuildResource> operation() {
        return client.builds();
    }
}

/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.DoneableRoute;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Route}s.
 */
public class RouteOperator extends AbstractResourceOperator<OpenShiftClient, Route, RouteList, DoneableRoute, Resource<Route, DoneableRoute>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The OpenShift client
     */
    public RouteOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "Route");
    }

    @Override
    protected MixedOperation<Route, RouteList, DoneableRoute, Resource<Route, DoneableRoute>> operation() {
        return client.routes();
    }
}

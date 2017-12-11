package io.enmasse.barnabas.controller.cluster.operations.kubernetes;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.enmasse.barnabas.controller.cluster.operations.Operation;
import io.vertx.core.Vertx;

public abstract class K8sOperation implements Operation {
    protected K8sOperation() {
    }
}

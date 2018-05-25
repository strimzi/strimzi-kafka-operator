/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.api.model.DoneableDeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code DeploymentConfigs}s.
 */
public class DeploymentConfigOperator extends AbstractScalableResourceOperator<OpenShiftClient, DeploymentConfig, DeploymentConfigList, DoneableDeploymentConfig, DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public DeploymentConfigOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "DeploymentConfig");
    }

    @Override
    protected MixedOperation<DeploymentConfig, DeploymentConfigList, DoneableDeploymentConfig, DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>> operation() {
        return client.deploymentConfigs();
    }

    @Override
    protected Integer currentScale(String namespace, String name) {
        DeploymentConfig deploymentConfig = get(namespace, name);
        if (deploymentConfig != null) {
            return deploymentConfig.getSpec().getReplicas();
        } else {
            return null;
        }
    }

    @Override
    protected Future<ReconcileResult<DeploymentConfig>> internalPatch(String namespace, String name, DeploymentConfig current, DeploymentConfig desired) {
        desired.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(current.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        return super.internalPatch(namespace, name, current, desired);
    }
}

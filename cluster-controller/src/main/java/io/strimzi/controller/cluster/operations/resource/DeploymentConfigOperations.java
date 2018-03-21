/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.api.model.DoneableDeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.vertx.core.Vertx;

/**
 * Operations for {@code DeploymentConfigs}s.
 */
public class DeploymentConfigOperations extends AbstractScalableOperations<OpenShiftClient, DeploymentConfig, DeploymentConfigList, DoneableDeploymentConfig, DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>, Void> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public DeploymentConfigOperations(Vertx vertx, OpenShiftClient client) {
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
}

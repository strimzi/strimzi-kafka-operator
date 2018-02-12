/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
public class DeploymentConfigOperations extends AbstractScalableOperations<OpenShiftClient, DeploymentConfig, DeploymentConfigList, DoneableDeploymentConfig, DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>> {
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
}

/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

public class StrimziResource {
    private String namespace;
    private String name;
    private DeploymentOperator deploymentOperations;

    public StrimziResource(String namespace, String name, DeploymentOperator deploymentOperations) {
        this.namespace = namespace;
        this.name = name;
        this.deploymentOperations = deploymentOperations;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    public DeploymentOperator getDeploymentOperations() {
        return deploymentOperations;
    }
}

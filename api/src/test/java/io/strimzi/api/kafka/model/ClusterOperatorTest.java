/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

/**
 * The purpose of this test is to ensure:
 *
 * 1. we get a correct tree of POJOs when reading a JSON/YAML `Cluster Operator` resource.
 */
public class ClusterOperatorTest extends AbstractCrdTest<ClusterOperator, DeploymentBuilder> {
    public ClusterOperatorTest() {
        super(ClusterOperator.class, DeploymentBuilder.class);
    }
}

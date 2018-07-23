/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.ClassRule;

import static org.junit.Assert.assertNotNull;

public abstract class AbstractCrdIT {

    @ClassRule
    public static KubeClusterResource cluster = new KubeClusterResource();

    protected <T extends CustomResource> void createDelete(Class<T> resourceClass, String resource) {
        String ssStr = TestUtils.readResource(resourceClass, resource);
        assertNotNull("Class path resource " + resource + " was missing", ssStr);
        createDelete(ssStr);
        T model = TestUtils.fromYaml(resource, resourceClass, true);
        ssStr = TestUtils.toYamlString(model);
        try {
            createDelete(ssStr);
        } catch (Error | RuntimeException e) {
            System.err.println(ssStr);
            throw new AssertionError("Create delete failed after first round-trip -- maybe a problem with a defaulted value?", e);
        }
    }

    private void createDelete(String ssStr) {
        RuntimeException thrown = null;
        RuntimeException thrown2 = null;
        try {
            try {
                cluster.client().applyContent(ssStr);
            } catch (RuntimeException t) {
                thrown = t;
            }
        } finally {
            try {
                cluster.client().deleteContent(ssStr);
            } catch (RuntimeException t) {
                thrown2 = t;
            }
        }
        if (thrown != null) {
            if (thrown2 != null) {
                thrown.addSuppressed(thrown2);
            }
            throw thrown;
        } else if (thrown2 != null) {
            throw thrown2;
        }
    }
}

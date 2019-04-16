/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import org.junit.Assume;
import org.junit.Before;

import static org.junit.Assert.assertNotNull;

public abstract class AbstractCrdIT extends BaseITST {

    protected void assumeKube1_11Plus() {
        VersionInfo version = new DefaultKubernetesClient().getVersion();
        String minor = version.getMinor();
        Assume.assumeTrue("1".equals(version.getMajor())
                && Integer.parseInt(minor.substring(0, minor.indexOf('+'))) >= 11);
    }

    protected <T extends CustomResource> void createDelete(Class<T> resourceClass, String resource) {
        String ssStr = TestUtils.readResource(resourceClass, resource);
        assertNotNull("Class path resource " + resource + " was missing", ssStr);
        createDelete(ssStr);
        T model = TestUtils.fromYaml(resource, resourceClass, false);
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
                CLUSTER.cmdClient().applyContent(ssStr);
            } catch (RuntimeException t) {
                thrown = t;
            }
        } finally {
            try {
                CLUSTER.cmdClient().deleteContent(ssStr);
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

    @Before
    public void setupTests() {
        CLUSTER.before();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class AbstractCrdIT extends BaseITST {

    protected void assumeKube1_11Plus() {
        VersionInfo version = new DefaultKubernetesClient().getVersion();
        assumeTrue("1".equals(version.getMajor())
                && Integer.parseInt(version.getMinor().split("\\D")[0]) >= 11);
    }

    protected <T extends CustomResource> void createDelete(Class<T> resourceClass, String resource) {
        String ssStr = TestUtils.readResource(resourceClass, resource);
        assertThat("Class path resource " + resource + " was missing", ssStr, is(notNullValue()));
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
                cmdKubeClient().applyContent(ssStr);
            } catch (RuntimeException t) {
                thrown = t;
            }
        } finally {
            try {
                cmdKubeClient().deleteContent(ssStr);
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

    @BeforeEach
    public void setupTests() {
        kubeCluster().before();
    }
}

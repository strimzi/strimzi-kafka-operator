/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCrdIT {

    protected KubeClusterResource cluster = KubeClusterResource.getInstance();

    protected void assumeKube1_11Plus() {
        VersionInfo version = new DefaultKubernetesClient().getVersion();
        assumeTrue("1".equals(version.getMajor())
                && Integer.parseInt(version.getMinor().split("\\D")[0]) >= 11);
    }

    private <T extends CustomResource> T loadResource(Class<T> resourceClass, String resource) {
        String ssStr = TestUtils.readResource(resourceClass, resource);
        assertThat("Class path resource " + resource + " was missing", ssStr, is(notNullValue()));
        createDelete(ssStr);
        return TestUtils.fromYaml(resource, resourceClass, false);
    }

    protected <T extends CustomResource> void createDelete(Class<T> resourceClass, String resource) {
        T model = loadResource(resourceClass, resource);
        String modelStr = TestUtils.toYamlString(model);
        assertDoesNotThrow(() -> createDelete(modelStr), "Create delete failed after first round-trip -- maybe a problem with a defaulted value?\nApplied string: " + modelStr);
    }

    private void createDelete(String ssStr) {
        RuntimeException creationException = null;
        RuntimeException deletionException = null;
        try {
            try {
                cmdKubeClient().applyContent(ssStr);
            } catch (RuntimeException t) {
                creationException = t;
            }
        } finally {
            try {
                cmdKubeClient().deleteContent(ssStr);
            } catch (RuntimeException t) {
                deletionException = t;
            }
        }
        if (creationException != null) {
            if (deletionException != null) {
                creationException.addSuppressed(deletionException);
            }
            throw creationException;
        } else if (deletionException != null) {
            throw deletionException;
        }
    }

    protected <T extends CustomResource> void createScaleDelete(Class<T> resourceClass, String resource) {
        T model = loadResource(resourceClass, resource);
        String modelKind = model.getKind();
        String modelName = model.getMetadata().getName();
        String modelStr = TestUtils.toYamlString(model);
        createScaleDelete(modelKind, modelName, modelStr);
    }

    private void createScaleDelete(String kind, String name, String ssStr) {
        RuntimeException creationOrScaleException = null;
        RuntimeException deletionException = null;
        try {
            try {
                cmdKubeClient().applyContent(ssStr);
                cmdKubeClient().scaleByName(kind, name, 10);
            } catch (RuntimeException t) {
                creationOrScaleException = t;
            }
        } finally {
            try {
                cmdKubeClient().deleteContent(ssStr);
            } catch (RuntimeException t) {
                deletionException = t;
            }
        }
        if (creationOrScaleException != null) {
            if (deletionException != null) {
                creationOrScaleException.addSuppressed(deletionException);
            }
            throw creationOrScaleException;
        } else if (deletionException != null) {
            throw deletionException;
        }
    }

    protected void assertMissingRequiredPropertiesMessage(String message, String... requiredProperties) {
        for (String requiredProperty: requiredProperties) {
            assertThat("Could not find" + requiredProperty + " in message: " + message, message, anyOf(
                    containsStringIgnoringCase(requiredProperty + " in body is required"),
                    containsStringIgnoringCase(requiredProperty + ": Required value")
            ));
        }
    }

    protected void waitForCrd(String resource, String name) {
        cluster.cmdClient().waitFor(resource, name, crd -> {
            JsonNode json = (JsonNode) crd;
            if (json != null
                    && json.hasNonNull("status")
                    && json.get("status").hasNonNull("conditions")) {
                return true;
            }

            return false;
        });
    }

    @BeforeEach
    public void setupTests() {
        cluster.before();
    }
}

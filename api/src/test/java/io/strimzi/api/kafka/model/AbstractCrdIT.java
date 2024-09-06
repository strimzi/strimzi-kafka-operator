/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.test.TestUtils;
import io.strimzi.test.interfaces.TestSeparator;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCrdIT implements TestSeparator {
    protected KubernetesClient client;

    protected <T extends CustomResource> void createDeleteCustomResource(String resourceName) {
        File resourceFile = new File(this.getClass().getResource(resourceName).getPath());
        createDelete(resourceFile);
    }

    private void createDelete(File resourceFile) {
        RuntimeException creationException = null;
        RuntimeException deletionException = null;

        try (FileInputStream fis = new FileInputStream(resourceFile)) {
            try {
                client.load(fis).create();
            } catch (RuntimeException t) {
                creationException = t;
            }
        } catch (IOException e)   {
            throw new RuntimeException("Failed to load the resource", e);
        } finally {
            try (FileInputStream fis = new FileInputStream(resourceFile)) {
                client.load(fis).delete();
            } catch (IOException e)   {
                throw new RuntimeException("Failed to load the resource", e);
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
        T model = TestUtils.fromYaml(resource, resourceClass);
        String apiVersion = model.getApiVersion();
        String kind = model.getKind();
        String resourceName = model.getMetadata().getName();
        String resourceYamlAsString = TestUtils.toYamlString(model);
        createScaleDelete(apiVersion, kind, resourceName, resourceYamlAsString);
    }

    private void createScaleDelete(String apiVersion, String kind, String name, String resourceYamlAsString) {
        RuntimeException creationOrScaleException = null;
        RuntimeException deletionException = null;

        try (ByteArrayInputStream bais = new ByteArrayInputStream(resourceYamlAsString.getBytes(StandardCharsets.UTF_8))) {
            try {
                client.load(bais).create();
                client.genericKubernetesResources(apiVersion, kind).withName(name).scale(10);
            } catch (RuntimeException t) {
                creationOrScaleException = t;
            }
        } catch (IOException e)   {
            throw new RuntimeException("Failed to load the resource", e);
        } finally {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(resourceYamlAsString.getBytes(StandardCharsets.UTF_8))) {
                client.load(bais).delete();
            } catch (IOException e)   {
                throw new RuntimeException("Failed to load the resource", e);
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
                    containsStringIgnoringCase(requiredProperty + ": Required value"),
                    containsStringIgnoringCase(requiredProperty.substring(requiredProperty.lastIndexOf(".") + 1) + ": Required value"),
                    containsStringIgnoringCase("missing required field \"" + requiredProperty + "\""),
                    containsStringIgnoringCase("missing required field \"" + requiredProperty.substring(requiredProperty.lastIndexOf(".") + 1) + "\"")
            ));
        }
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionSpec;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;


/**
 * The purpose of this test is to check that all the resources in the
 * {@code ../packaging/examples} directory are valid.
 */
public class ExamplesTest {
    /**
     * Recurse through the examples directory looking for resources of the right type
     * and validating them.
     */
    @Test
    public void examples() {
        validateRecursively(new File(TestUtils.USER_PATH + "/../packaging/examples"));
    }

    private void validateRecursively(File directory) {
        for (File f : directory.listFiles()) {
            if (f.isDirectory()) {
                if (f.getAbsolutePath().contains("packaging/examples/metrics/grafana") || f.getAbsolutePath().contains("packaging/examples/metrics/prometheus"))  {
                    continue;
                } else {
                    validateRecursively(f);
                }
            } else if (f.isFile()
                    && (f.getName().endsWith(".yaml") || f.getName().endsWith(".yml"))) {
                validate(f);
            }
        }
    }

    private void validate(File f) {
        try {
            ObjectMapper mapper = new YAMLMapper();
            final String content = TestUtils.readFile(f);
            validate(content);
        } catch (Exception | AssertionError e) {
            throw new AssertionError("Invalid example yaml in " + f.getPath() + ": " + e.getMessage(), e);
        }
    }

    private void validate(String content) {
        // Validate first using the Fabric8 KubernetesResource
        // This uses a custom deserializer which knows about all the built-in
        // k8s and os kinds, plus the custom kinds registered via Crds
        // But the custom deserializer always allows unknown properties
        KubernetesResource resource = new KubernetesClientBuilder().build().getKubernetesSerialization().convertValue(content, KubernetesResource.class);
        recurseForAdditionalProperties(new Stack(), resource);
    }

    /**
     * Recursively search an object tree checking for POJOs annotated with @JsonAnyGetter.
     * These are likely erroneous YAML.
     */
    private void recurseForAdditionalProperties(Stack<String> path, Object resource) {
        try {
            Class<?> cls = resource.getClass();
            for (Method method : cls.getMethods()) {
                checkForJsonAnyGetter(path, resource, cls, method);
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkForJsonAnyGetter(Stack<String> path, Object resource, Class<?> cls, Method method) throws IllegalAccessException, InvocationTargetException {
        if (method.isAnnotationPresent(JsonAnyGetter.class)) {
            Map additionalProperties = (Map) method.invoke(resource);
            if (CustomResourceDefinitionSpec.class.equals(cls)) {
                additionalProperties = new HashMap(additionalProperties);
                additionalProperties.remove("validation");
            }
            if (additionalProperties != null && !additionalProperties.isEmpty()) {
                fail("object at path " + path.stream().collect(Collectors.joining("/")) + " has additional properties " + additionalProperties.keySet());
            }
        } else {
            if (isGetter(method)) {
                method.setAccessible(true);
                Object result = method.invoke(resource);
                if (result != null
                    && !result.getClass().isPrimitive()
                    && !result.getClass().isEnum()
                    && !result.getClass().equals(Collections.emptyMap().getClass())) {
                    path.push(method.getName());
                    recurseForAdditionalProperties(path, result);
                    path.pop();
                }
            }
        }
    }

    private boolean isGetter(Method method) {
        String name = method.getName();
        boolean hasGetterName = !name.equals("getClass")
            && (name.startsWith("get") && name.length() > 3
                || name.startsWith("is") && name.length() > 2);
        return method.getParameterCount() == 0
                && hasGetterName
                && !Modifier.isStatic(method.getModifiers())
                && !method.getReturnType().equals(void.class);
    }
}

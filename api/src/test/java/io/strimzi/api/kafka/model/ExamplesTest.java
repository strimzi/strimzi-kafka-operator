/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionSpec;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.Crds;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;


/**
 * The purpose of this test is to check that all the resources in the
 * {@code ../examples} directory are valid.
 */
public class ExamplesTest {

    static {
        Crds.registerCustomKinds();
    }

    private static final Pattern PARAMETER_PATTERN = Pattern.compile("\\$\\{\\{(.+?)\\}?\\}");

    /**
     * Recurse through the examples directory looking for resources of the right type
     * and validating them.
     */
    @Test
    public void examples() throws Exception {
        validateRecursively(new File(TestUtils.USER_PATH + "/../examples"));
    }

    private void validateRecursively(File directory) {
        for (File f : directory.listFiles()) {
            if (f.isDirectory()) {
                if (f.getAbsolutePath().contains("examples/metrics/grafana") || f.getAbsolutePath().contains("examples/metrics/prometheus"))  {
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
            JsonNode rootNode = mapper.readTree(content);
            String resourceKind = getKind(rootNode);
            if ("Template".equals(resourceKind)) {
                validateTemplate(rootNode);
            } else {
                validate(content);
            }
        } catch (Exception | AssertionError e) {
            throw new AssertionError("Invalid example yaml in " + f.getPath() + ": " + e.getMessage(), e);
        }
    }

    private void validate(String content) {
        // Validate first using the Fabric8 KubernetesResource
        // This uses a custom deserializer which knows about all the built-in
        // k8s and os kinds, plus the custom kinds registered via Crds
        // But the custom deserializer always allows unknown properties
        KubernetesResource resource = TestUtils.fromYamlString(content, KubernetesResource.class, false);
        recurseForAdditionalProperties(new Stack(), resource);
    }

    /**
     * Recursively search an object tree checking for POJOs annotated with @JsonAnyGetter.
     * These are likely erroneous YAML.
     */
    private void recurseForAdditionalProperties(Stack<String> path, Object resource) {
        try {
            Class<?> cls = resource.getClass();
            if (RoleBinding.class.equals(cls)
                    || ClusterRoleBinding.class.equals(cls)) {
                // XXX: hack because fabric8 RoleBinding reflect the openshift role binding API
                // not the k8s one, and has an unexpected apiGroup property
                return;
            }
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
                    && !result.getClass().isEnum()) {
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

    private void validateTemplate(JsonNode rootNode) throws JsonProcessingException {
        JsonNode parameters = rootNode.get("parameters");
        Map<String, Object> params = new HashMap<>();
        for (JsonNode parameter : parameters) {
            String name = parameter.get("name").asText();
            Object value;
            JsonNode valueNode = parameter.get("value");
            switch (valueNode.getNodeType()) {
                case NULL:
                    value = null;
                    break;
                case NUMBER:
                case BOOLEAN:
                    value = valueNode.toString();
                    break;
                case STRING:
                    value = valueNode.asText();
                    break;
                default:
                    throw new RuntimeException("Unsupported JSON type " + valueNode.getNodeType());
            }
            params.put(name, value);
        }
        for (JsonNode object : rootNode.get("objects")) {
            String s = new YAMLMapper().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES).enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(object);
            Matcher matcher = PARAMETER_PATTERN.matcher(s);
            StringBuilder sb = new StringBuilder();
            int last = 0;
            while (matcher.find()) {
                sb.append(s, last, matcher.start());
                String paramName = matcher.group(1);
                sb.append(params.get(paramName));
                last = matcher.end();
            }
            sb.append(s.substring(last));
            String yamlContent = sb.toString();
            validate(yamlContent);
        }
    }

    private String getKind(JsonNode rootNode) {
        return rootNode.get("kind").asText();
    }
}

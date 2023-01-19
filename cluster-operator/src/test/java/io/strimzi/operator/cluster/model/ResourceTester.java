/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class ResourceTester<R extends HasMetadata, M extends AbstractModel> {

    private final KafkaVersion.Lookup lookup;
    private final Class<R> cls;
    private final String prefix;
    private final BiFunction<R, KafkaVersion.Lookup, M> fromK8sResource;
    private M model;
    private R cr;
    private String resourceName;

    ResourceTester(Class<R> cls, KafkaVersion.Lookup lookup, BiFunction<R, KafkaVersion.Lookup, M> fromK8sResource, String prefix) {
        this.lookup = lookup;
        this.cls = cls;
        this.fromK8sResource = fromK8sResource;
        this.prefix = prefix;
        beforeEach();
    }

    static <T> T fromYaml(URL url, Class<T> c) {
        if (url == null) {
            return null;
        }
        ObjectMapper mapper = new YAMLMapper();
        try {
            return mapper.readValue(url, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> String toYamlString(T instance) {
        ObjectMapper mapper = new YAMLMapper();
        try {
            return mapper.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected void assertDesiredModel(String suffix, Function<M, ?> fn) throws IOException {
        assertThat("The resource " + resourceName + " does not exist", model, is(notNullValue()));
        String content = readResource(prefix + suffix);
        if (content != null) {
            String ssStr = toYamlString(fn.apply(model));
            assertThat(ssStr.trim(), is(content.trim()));
        } else {
            fail("The resource " + prefix + suffix + " does not exist");
        }
    }

    protected void assertDesiredResource(String suffix, Function<R, ?> fn) throws IOException {
        assertThat("The resource " + resourceName + " does not exist", model, is(notNullValue()));
        String content = readResource(prefix + suffix);
        if (content != null) {
            String ssStr = toYamlString(fn.apply(cr));
            assertThat(ssStr.trim(), is(content.trim()));
        } else {
            fail("The resource " + prefix + suffix + " does not exist");
        }
    }

    private String readResource(String resource) throws IOException {
        InputStream expectedStream = getClass().getResourceAsStream(resource);
        if (expectedStream != null) {
            try {
                StringBuilder sb = new StringBuilder();
                BufferedReader reader = new BufferedReader(new InputStreamReader(expectedStream));
                String line = reader.readLine();
                while (line != null) {
                    sb.append(line).append("\n");
                    line = reader.readLine();
                }
                return sb.toString();
            } finally {
                expectedStream.close();
            }
        } else {
            return null;
        }
    }

    public void beforeEach() {
        // Parse resource into CM
        try {
            resourceName = CustomResource.class.isAssignableFrom(cls) ?
                    prefix + "-" + cls.newInstance().getKind() + ".yaml" :
                    prefix + "-" + cls.getSimpleName() + ".yaml";
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        URL resource = getClass().getResource(resourceName);
        if (resource == null) {
            model = null;
        } else {
            cr = fromYaml(resource, cls);
            // Construct the desired resources from the CM
            model = fromK8sResource.apply(cr, lookup);
        }
    }
}

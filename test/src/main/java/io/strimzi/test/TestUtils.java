/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public final class TestUtils {

    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String CRD_TOPIC = "../examples/install/topic-operator/04-Crd-kafkatopic.yaml";

    public static final String CRD_KAFKA = "../examples/install/cluster-operator/04-Crd-kafka.yaml";

    public static final String CRD_KAFKA_CONNECT = "../examples/install/cluster-operator/04-Crd-kafkaconnect.yaml";

    public static final String CRD_KAFKA_CONNECT_S2I = "../examples/install/cluster-operator/04-Crd-kafkaconnects2i.yaml";

    private TestUtils() {
        // All static methods
    }

    public static final String KAFKA_USER_CRD = "../examples/install/cluster-operator/04-Crd-kafkauser.yaml";

    /** Returns a Map of the given sequence of key, value pairs. */
    public static <T> Map<T, T> map(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<T, T> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    /**
     * Poll the given {@code ready} function every {@code pollIntervalMs} milliseconds until it returns true,
     * or throw a TimeoutException if it doesn't returns true within {@code timeoutMs} milliseconds.
     * @return The remaining time left until timeout occurs
     * (helpful if you have several calls which need to share a common timeout),
     * */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        return waitFor(description, pollIntervalMs, timeoutMs, ready, () -> { });
    }

    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready, Runnable onTimeout) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            boolean result = ready.getAsBoolean();
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                onTimeout.run();
                throw new TimeoutException("Timeout after " + timeoutMs + " ms waiting for " + description + " to be ready");
            }
            long sleepTime = Math.min(pollIntervalMs, timeLeft);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} not ready, will try again in {} ms ({}ms till timeout)", description, sleepTime, timeLeft);
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return deadline - System.currentTimeMillis();
            }
        }
    }

    public static String indent(String s) {
        StringBuilder sb = new StringBuilder();
        String[] lines = s.split("[\n\r]");
        for (String line : lines) {
            sb.append("    ").append(line).append(System.lineSeparator());
        }
        return sb.toString();
    }

    public static String getFileAsString(String filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)), "UTF-8");
        } catch (IOException e) {
            LOGGER.info("File with path {} not found", filePath);
        }
        return "";
    }

    public static String changeOrgAndTag(String image, String newOrg, String newTag) {
        return image.replaceFirst("^strimzi/", newOrg + "/").replaceFirst(":[^:]+$", ":" + newTag);
    }

    public static String changeOrgAndTag(String image) {
        String strimziOrg = "strimzi";
        String strimziTag = "latest";
        String dockerOrg = System.getenv().getOrDefault("DOCKER_ORG", strimziOrg);
        String dockerTag = System.getenv().getOrDefault("DOCKER_TAG", strimziTag);
        return changeOrgAndTag(image, dockerOrg, dockerTag);
    }

    /**
     * Read the classpath resource with the given resourceName and return the content as a String
     * @param cls The class relative to which the resource will be loaded.
     * @param resourceName The name of the resource
     * @return The resource content
     * @throws IOException
     */
    public static String readResource(Class<?> cls, String resourceName) {
        try {
            URL url = cls.getResource(resourceName);
            if (url == null) {
                return null;
            } else {
                return new String(
                        Files.readAllBytes(Paths.get(
                                url.toURI())),
                        StandardCharsets.UTF_8);
            }
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static String readFile(File file) {
        try {
            if (file == null) {
                return null;
            } else {
                return new String(
                        Files.readAllBytes(file.toPath()),
                        StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Assert that the given actual string is the same as content of the
     * the classpath resource resourceName.
     * @param cls The class relative to which the resource will be loaded.
     * @param resourceName The name of the resource
     * @param actual The actual
     * @throws IOException
     */
    public static void assertResourceMatch(Class<?> cls, String resourceName, String actual) throws IOException {
        String r = readResource(cls, resourceName);
        assertEquals(r, actual);
    }


    public static <T> Set<T> set(T... elements) {
        return new HashSet(asList(elements));
    }

    public static <T> T fromYaml(String resource, Class<T> c) {
        return fromYaml(resource, c, false);
    }

    public static <T> T fromYaml(String resource, Class<T> c, boolean ignoreUnknownProperties) {
        URL url = c.getResource(resource);
        if (url == null) {
            return null;
        }
        ObjectMapper mapper = new YAMLMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, !ignoreUnknownProperties);
        try {
            return mapper.readValue(url, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromYamlString(String yamlContent, Class<T> c) {
        return fromYamlString(yamlContent, c, false);
    }

    public static <T> T fromYamlString(String yamlContent, Class<T> c, boolean ignoreUnknownProperties) {
        ObjectMapper mapper = new YAMLMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, !ignoreUnknownProperties);
        try {
            return mapper.readValue(yamlContent, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromYamlFile(String filename, Class<T> c) {
        return fromYamlFile(new File(filename), c, false);
    }

    public static <T> T fromYamlFile(String filename, Class<T> c, boolean ignoreUnknownProperties) {
        return fromYamlFile(new File(filename), c, ignoreUnknownProperties);
    }

    public static <T> T fromYamlFile(File yamlFile, Class<T> c, boolean ignoreUnknownProperties) {
        ObjectMapper mapper = new YAMLMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, !ignoreUnknownProperties);
        try {
            return mapper.readValue(yamlFile, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> String toYamlString(T instance) {
        ObjectMapper mapper = new YAMLMapper()
                .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            return mapper.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /** @deprecated you should be using yaml, no json */
    @Deprecated
    public static <T> T fromJson(String json, Class<T> c) {
        if (json == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return mapper.readValue(json, c);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJsonString(Object instance) {
        ObjectMapper mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            return mapper.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assumeLinux() {
        assumeTrue(System.getProperty("os.name").contains("nux"));
    }

    /** Map Streams utility methods */
    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
    }
}

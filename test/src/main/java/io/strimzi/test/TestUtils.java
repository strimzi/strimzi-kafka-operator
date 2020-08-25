/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.vertx.core.VertxException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public final class TestUtils {

    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    public static final String USER_PATH = System.getProperty("user.dir");

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String CRD_TOPIC = USER_PATH + "/../install/cluster-operator/043-Crd-kafkatopic.yaml";

    public static final String CRD_KAFKA = USER_PATH + "/../install/cluster-operator/040-Crd-kafka.yaml";

    public static final String CRD_KAFKA_CONNECT = USER_PATH + "/../install/cluster-operator/041-Crd-kafkaconnect.yaml";

    public static final String CRD_KAFKA_CONNECT_S2I = USER_PATH + "/../install/cluster-operator/042-Crd-kafkaconnects2i.yaml";

    public static final String CRD_KAFKA_USER = USER_PATH + "/../install/cluster-operator/044-Crd-kafkauser.yaml";

    public static final String CRD_KAFKA_MIRROR_MAKER = USER_PATH + "/../install/cluster-operator/045-Crd-kafkamirrormaker.yaml";

    public static final String CRD_KAFKA_BRIDGE = USER_PATH + "/../install/cluster-operator/046-Crd-kafkabridge.yaml";

    public static final String CRD_KAFKA_MIRROR_MAKER_2 = USER_PATH + "/../install/cluster-operator/048-Crd-kafkamirrormaker2.yaml";

    public static final String CRD_KAFKA_CONNECTOR = USER_PATH + "/../install/cluster-operator/047-Crd-kafkaconnector.yaml";

    public static final String CRD_KAFKA_REBALANCE = USER_PATH + "/../install/cluster-operator/049-Crd-kafkarebalance.yaml";

    private TestUtils() {
        // All static methods
    }

    /** Returns a Map of the given sequence of key, value pairs. */
    @SafeVarargs
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
     * or throw a WaitException if it doesn't returns true within {@code timeoutMs} milliseconds.
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
            boolean result;
            try {
                result = ready.getAsBoolean();
            } catch (Exception e) {
                result = false;
            }
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                onTimeout.run();
                WaitException waitException = new WaitException("Timeout after " + timeoutMs + " ms waiting for " + description);
                waitException.printStackTrace();
                throw waitException;
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
            LOGGER.info(filePath);
            return new String(Files.readAllBytes(Paths.get(filePath)), "UTF-8");
        } catch (IOException e) {
            LOGGER.info("File with path {} not found", filePath);
        }
        return "";
    }

    /**
     * Read the classpath resource with the given resourceName and return the content as a String
     * @param cls The class relative to which the resource will be loaded.
     * @param resourceName The name of the resource
     * @return The resource content
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

    /**
     * Read loaded resource as an InputStream and return the content as a String
     * @param stream Loaded resource
     * @return The resource content
     */
    public static String readResource(InputStream stream) {
        StringBuilder textBuilder = new StringBuilder();
        try (Reader reader = new BufferedReader(new InputStreamReader(
                stream, Charset.forName(StandardCharsets.UTF_8.name()))
        )) {
            int character = 0;
            while ((character = reader.read()) != -1) {
                textBuilder.append((char) character);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return textBuilder.toString();
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
        assertThat(actual, is(r));
    }

    @SafeVarargs
    public static <T> Set<T> set(T... elements) {
        return new HashSet<>(asList(elements));
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

    public static <T> String toYamlString(T instance) {
        ObjectMapper mapper = new YAMLMapper()
                .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        try {
            return mapper.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T configFromYaml(String yamlPath, Class<T> c) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(new File(yamlPath), c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
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

    /** Map Streams utility methods */
    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    /** Method to create and write file */
    public static void writeFile(String filePath, String text) {
        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(filePath), StandardCharsets.UTF_8));
            writer.write(text);
        } catch (IOException e) {
            LOGGER.info("Exception during writing text in file");
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    /**
     * Changes the {@code subject} of the RoleBinding in the given YAML resource to be the
     * {@code strimzi-cluster-operator} {@code ServiceAccount} in the given namespace.
     * @param roleBindingFile
     * @param namespace
     * @return role
     */
    public static String changeRoleBindingSubject(File roleBindingFile, String namespace) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(roleBindingFile);
            ArrayNode subjects = (ArrayNode) node.get("subjects");
            ObjectNode subject = (ObjectNode) subjects.get(0);
            subject.put("kind", "ServiceAccount")
                    .put("name", "strimzi-cluster-operator")
                    .put("namespace", namespace);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String changeDeploymentNamespaceUpgrade(File deploymentFile, String namespace) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(deploymentFile);
            // Change the docker org of the images in the 060-deployment.yaml
            ObjectNode containerNode = (ObjectNode) node.at("/spec/template/spec/containers").get(0);
            for (JsonNode envVar : containerNode.get("env")) {
                String varName = envVar.get("name").textValue();
                if (varName.matches("STRIMZI_NAMESPACE")) {
                    // Replace all the default images with ones from the $DOCKER_ORG org and with the $DOCKER_TAG tag
                    ((ObjectNode) envVar).remove("valueFrom");
                    ((ObjectNode) envVar).put("value", namespace);
                }
                if (varName.matches("STRIMZI_LOG_LEVEL")) {
                    ((ObjectNode) envVar).put("value", "DEBUG");
                }
            }
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getContent(File file, Function<JsonNode, String> edit) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(file);
            return edit.apply(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> parseImageMap(String str) {
        if (str != null) {
            StringTokenizer tok = new StringTokenizer(str, ", \t\n\r");
            HashMap<String, String> map = new HashMap<>();
            while (tok.hasMoreTokens()) {
                String versionImage = tok.nextToken();
                int endIndex = versionImage.indexOf('=');
                String version = versionImage.substring(0, endIndex);
                String image = versionImage.substring(endIndex + 1);
                map.put(version.trim(), image.trim());
            }
            return Collections.unmodifiableMap(map);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Repeat request n-times in a row in case of call failed
     *
     * @param retry count of remaining retries
     * @param fn    request function
     * @return The result of the successful call to {@code fn}.
     */
    public static <T> T doRequestTillSuccess(int retry, Callable<T> fn, Optional<Runnable> reconnect) throws Exception {
        try {
            return fn.call();
        } catch (Exception ex) {
            if (ex.getCause() instanceof VertxException && ex.getCause().getMessage().contains("Connection was closed")) {
                if (reconnect.isPresent()) {
                    LOGGER.warn("connection was closed, trying to reconnect...");
                    reconnect.get().run();
                }
            }
            if ((ex.getCause() instanceof UnknownHostException || ex.getCause() instanceof IllegalStateException) && retry > 0) {
                try {
                    LOGGER.info("{} remaining iterations", retry);
                    return doRequestTillSuccess(retry - 1, fn, reconnect);
                } catch (Exception ex2) {
                    throw ex2;
                }
            } else {
                LOGGER.info(ex.getClass().getName());
                if (ex.getCause() != null) {
                    ex.getCause().printStackTrace();
                } else {
                    ex.printStackTrace();
                }
                throw ex;
            }
        }
    }
}

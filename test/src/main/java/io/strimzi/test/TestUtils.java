/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
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
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public final class TestUtils {

    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    public static final String USER_PATH = System.getProperty("user.dir");

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String CRD_TOPIC = USER_PATH + "/../packaging/install/cluster-operator/043-Crd-kafkatopic.yaml";

    public static final String CRD_KAFKA = USER_PATH + "/../packaging/install/cluster-operator/040-Crd-kafka.yaml";

    public static final String CRD_KAFKA_CONNECT = USER_PATH + "/../packaging/install/cluster-operator/041-Crd-kafkaconnect.yaml";

    public static final String CRD_KAFKA_USER = USER_PATH + "/../packaging/install/cluster-operator/044-Crd-kafkauser.yaml";

    public static final String CRD_KAFKA_MIRROR_MAKER = USER_PATH + "/../packaging/install/cluster-operator/045-Crd-kafkamirrormaker.yaml";

    public static final String CRD_KAFKA_BRIDGE = USER_PATH + "/../packaging/install/cluster-operator/046-Crd-kafkabridge.yaml";

    public static final String CRD_KAFKA_MIRROR_MAKER_2 = USER_PATH + "/../packaging/install/cluster-operator/048-Crd-kafkamirrormaker2.yaml";

    public static final String CRD_KAFKA_CONNECTOR = USER_PATH + "/../packaging/install/cluster-operator/047-Crd-kafkaconnector.yaml";

    public static final String CRD_KAFKA_REBALANCE = USER_PATH + "/../packaging/install/cluster-operator/049-Crd-kafkarebalance.yaml";

    public static final String CRD_STRIMZI_POD_SET = USER_PATH + "/../packaging/install/cluster-operator/042-Crd-strimzipodset.yaml";

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
     * or throw a WaitException if it doesn't return true within {@code timeoutMs} milliseconds.
     * @return The remaining time left until timeout occurs
     * (helpful if you have several calls which need to share a common timeout),
     * */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        return waitFor(description, pollIntervalMs, timeoutMs, ready, () -> { });
    }

    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready, Runnable onTimeout) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;

        String exceptionMessage = null;
        String previousExceptionMessage = null;

        // in case we are polling every 1s, we want to print exception after x tries, not on the first try
        // for minutes poll interval will 2 be enough
        int exceptionAppearanceCount = Duration.ofMillis(pollIntervalMs).toMinutes() > 0 ? 2 : Math.max((int) (timeoutMs / pollIntervalMs) / 4, 2);
        int exceptionCount = 0;
        int newExceptionAppearance = 0;

        StringWriter stackTraceError = new StringWriter();

        while (true) {
            boolean result;
            try {
                result = ready.getAsBoolean();
            } catch (Exception e) {
                exceptionMessage = e.getMessage();

                if (++exceptionCount == exceptionAppearanceCount && exceptionMessage != null && exceptionMessage.equals(previousExceptionMessage)) {
                    LOGGER.error("While waiting for {} exception occurred: {}", description, exceptionMessage);
                    // log the stacktrace
                    e.printStackTrace(new PrintWriter(stackTraceError));
                } else if (exceptionMessage != null && !exceptionMessage.equals(previousExceptionMessage) && ++newExceptionAppearance == 2) {
                    previousExceptionMessage = exceptionMessage;
                }

                result = false;
            }
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                if (exceptionCount > 1) {
                    LOGGER.error("Exception waiting for {}, {}", description, exceptionMessage);

                    if (!stackTraceError.toString().isEmpty()) {
                        // printing handled stacktrace
                        LOGGER.error(stackTraceError.toString());
                    }
                }
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
            return Files.readString(Paths.get(filePath));
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
                return Files.readString(Paths.get(
                        url.toURI()));
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
                stream, StandardCharsets.UTF_8)
        )) {
            int character;
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
                return Files.readString(file.toPath());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        return configFromYaml(new File(yamlPath), c);
    }

    public static ConfigMap configMapFromYaml(String yamlPath, String name) {
        try {
            YAMLFactory yaml = new YAMLFactory();
            ObjectMapper mapper = new ObjectMapper(yaml);
            YAMLParser yamlParser = yaml.createParser(new File(yamlPath));
            List<ConfigMap> list = mapper.readValues(yamlParser, new TypeReference<ConfigMap>() { }).readAll();
            Optional<ConfigMap> cmOpt = list.stream().filter(cm -> "ConfigMap".equals(cm.getKind()) && name.equals(cm.getMetadata().getName())).findFirst();
            if (cmOpt.isPresent()) {
                return cmOpt.get();
            } else {
                LOGGER.warn("ConfigMap {} not found in file {}", name, yamlPath);
                return null;
            }
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static <T> T configFromYaml(File yamlFile, Class<T> c) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(yamlFile, c);
        } catch (InvalidFormatException e) {
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

    public static void checkOwnerReference(HasMetadata resource, HasMetadata parent)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));

        OwnerReference or = resource.getMetadata().getOwnerReferences().get(0);

        assertThat(or.getApiVersion(), is(parent.getApiVersion()));
        assertThat(or.getKind(), is(parent.getKind()));
        assertThat(or.getName(), is(parent.getMetadata().getName()));
        assertThat(or.getUid(), is(parent.getMetadata().getUid()));
        assertThat(or.getBlockOwnerDeletion(), is(false));
        assertThat(or.getController(), is(false));
    }

    /**
     * Changes the {@code subject} of the RoleBinding in the given YAML resource to be the
     * {@code strimzi-cluster-operator} {@code ServiceAccount} in the given namespace.
     *
     * @param roleBindingFile   The RoleBinding YAML file to load and change
     * @param namespace         Namespace of the service account which should be the subject of this RoleBinding
     *
     * @return Modified RoleBinding resource YAML
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
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.jayway.jsonpath.JsonPath;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class StUtils {

    private static final Logger LOGGER = LogManager.getLogger(StUtils.class);

    private static final Pattern KAFKA_COMPONENT_PATTERN = Pattern.compile("([^-|^_]*?)(?<kafka>[-|_]kafka[-|_])(?<version>.*)$");

    private static final Pattern IMAGE_PATTERN_FULL_PATH = Pattern.compile("^(?<registry>[^/]*)/(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");
    private static final Pattern IMAGE_PATTERN = Pattern.compile("^(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");

    private static final Pattern VERSION_IMAGE_PATTERN = Pattern.compile("(?<version>[0-9.]+)=(?<image>[^\\s]*)");

    private static final Pattern BETWEEN_JSON_OBJECTS_PATTERN = Pattern.compile("}[\\n\\r]+\\{");
    private static final Pattern ALL_BEFORE_JSON_PATTERN = Pattern.compile("(.*\\s)}, \\{", Pattern.DOTALL);

    private StUtils() { }

    /**
     * Method for check if test is allowed on specific testing environment
     * @param envVariableForCheck environment variable which is specific for a specific environment
     * @return true if test is allowed, false if not
     */
    public static boolean isAllowOnCurrentEnvironment(String envVariableForCheck) {
        return System.getenv().get(envVariableForCheck) == null;
    }

    /**
     * The method to configure docker image to use proper docker registry, docker org and docker tag.
     * @param image Image that needs to be changed
     * @return Updated docker image with a proper registry, org, tag
     */
    public static String changeOrgAndTag(String image) {
        Matcher m = IMAGE_PATTERN_FULL_PATH.matcher(image);
        if (m.find()) {
            String registry = setImageProperties(m.group("registry"), Environment.STRIMZI_REGISTRY, Environment.STRIMZI_REGISTRY_DEFAULT);
            String org = setImageProperties(m.group("org"), Environment.STRIMZI_ORG, Environment.STRIMZI_ORG_DEFAULT);

            return registry + "/" + org + "/" + m.group("image") + ":" + buildTag(m.group("tag"));
        }
        m = IMAGE_PATTERN.matcher(image);
        if (m.find()) {
            String org = setImageProperties(m.group("org"), Environment.STRIMZI_ORG, Environment.STRIMZI_ORG_DEFAULT);

            return Environment.STRIMZI_REGISTRY + "/" + org + "/" + m.group("image") + ":"  + buildTag(m.group("tag"));
        }
        return image;
    }

    public static String changeOrgAndTagInImageMap(String imageMap) {
        Matcher m = VERSION_IMAGE_PATTERN.matcher(imageMap);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, m.group("version") + "=" + changeOrgAndTag(m.group("image")));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String setImageProperties(String current, String envVar, String defaultEnvVar) {
        if (!envVar.equals(defaultEnvVar) && !current.equals(envVar)) {
            return envVar;
        }
        return current;
    }

    private static String buildTag(String currentTag) {
        if (!currentTag.equals(Environment.STRIMZI_TAG) && !Environment.STRIMZI_TAG_DEFAULT.equals(Environment.STRIMZI_TAG)) {
            Matcher t = KAFKA_COMPONENT_PATTERN.matcher(currentTag);
            if (t.find()) {
                currentTag = Environment.STRIMZI_TAG + t.group("kafka") + t.group("version");
            } else {
                currentTag = Environment.STRIMZI_TAG;
            }
        }
        return currentTag;
    }

    public static List<ContainerEnvVar> createContainerEnvVarsFromMap(Map<String, String> envVars) {
        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            testEnvs.add(new ContainerEnvVarBuilder()
                .withName(entry.getKey())
                .withValue(entry.getValue()).build());
        }
        return testEnvs;
    }

    public static String checkEnvVarInPod(String podName, String envVarName) {
        return kubeClient().getPod(podName).getSpec().getContainers().get(0).getEnv()
                .stream().filter(envVar -> envVar.getName().equals(envVarName)).findFirst().get().getValue();
    }

    /**
     * Translate key/value pairs formatted like properties into a Map
     * @param keyValuePairs Pairs in key=value format; pairs are separated by newlines
     * @return THe map of key/values
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> loadProperties(String keyValuePairs) {
        try {
            Properties actual = new Properties();
            actual.load(new StringReader(keyValuePairs));
            return (Map) actual;
        } catch (IOException e) {
            throw new AssertionError("Invalid Properties definition", e);
        }
    }

    /**
     * Get a Map of properties from an environment variable in json.
     * @param containerIndex name of the container
     * @param json The json from which to extract properties
     * @param envVar The environment variable name
     * @return The properties which the variable contains
     */
    public static Map<String, Object> getPropertiesFromJson(int containerIndex, String json, String envVar) {
        List<String> array = JsonPath.parse(json).read(globalVariableJsonPathBuilder(containerIndex, envVar));
        return StUtils.loadProperties(array.get(0));
    }

    /**
     * Get a jsonPath which can be used to extract envariable variables from a spec
     * @param containerIndex index of the container
     * @param envVar The environment variable name
     * @return The json path
     */
    public static String globalVariableJsonPathBuilder(int containerIndex, String envVar) {
        return "$.spec.containers[" + containerIndex + "].env[?(@.name=='" + envVar + "')].value";
    }

    public static Properties stringToProperties(String str) {
        Properties result = new Properties();
        List<String> list = getLinesWithoutCommentsAndEmptyLines(str);
        for (String line: list) {
            String[] split = line.split("=");
            result.put(split[0], split.length == 1 ? "" : split[1]);
        }
        return result;
    }

    public static Properties configMap2Properties(ConfigMap cm) {
        return stringToProperties(cm.getData().get("server.config"));
    }

    public static List<String> getLinesWithoutCommentsAndEmptyLines(String config) {
        List<String> allLines = Arrays.asList(config.split("\\r?\\n"));
        List<String> validLines = new ArrayList<>();

        for (String line : allLines)    {
            if (!line.replace(" ", "").startsWith("#") && !line.isEmpty())   {
                validLines.add(line.replace(" ", ""));
            }
        }
        return validLines;
    }

    public static JsonArray expectedServiceDiscoveryInfo(int port, String protocol, String auth, boolean tls) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("port", port);
        jsonObject.put(Constants.TLS_LISTENER_DEFAULT_NAME, tls);
        jsonObject.put("protocol", protocol);
        jsonObject.put("auth", auth);

        JsonArray jsonArray = new JsonArray();
        jsonArray.add(jsonObject);

        return jsonArray;
    }

    /**
     * Build jsonArray with data about service discovery based on pass configuration
     * @param plainEcryption plain listener encryption
     * @param tlsEncryption tls listener encryption
     * @param plainTlsAuth plain listener authentication
     * @param tlsTlsAuth tls listener authentication
     * @return builded jsonArray
     */
    public static JsonArray expectedServiceDiscoveryInfo(String plainEcryption, String tlsEncryption, boolean plainTlsAuth, boolean tlsTlsAuth) {
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(expectedServiceDiscoveryInfo(9092, "kafka", plainEcryption, plainTlsAuth).getValue(0));
        jsonArray.add(expectedServiceDiscoveryInfo(9093, "kafka", tlsEncryption, tlsTlsAuth).getValue(0));
        return jsonArray;
    }

    /**
     * Method for checking if JSON format logging is set for the {@code pods}
     * Steps:
     * 1. get log from pod
     * 2. find every occurrence of `}\n{` which will be replaced with `}, {` - by {@link #BETWEEN_JSON_OBJECTS_PATTERN}
     * 3. replace everything from beginning to the first proper JSON object with `{`- by {@link #ALL_BEFORE_JSON_PATTERN}
     * 4. also add `[` to beginning and `]` to the end of String to create proper JsonArray
     * 5. try to parse the JsonArray
     * @param pods snapshot of pods to be checked
     * @param containerName name of container from which to take the log
     */
    public static void checkLogForJSONFormat(Map<String, String> pods, String containerName) {
        //this is only for decrease the number of records - kafka have record/line, operators record/11lines
        String tail = "--tail=" + (containerName.contains("operator") ? "100" : "10");

        TestUtils.waitFor("for JSON log in " + pods, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            boolean isJSON = false;
            for (String podName : pods.keySet()) {
                String log = cmdKubeClient().execInCurrentNamespace(false, "logs", podName, "-c", containerName, tail).out();

                // remove incomplete JSON from the end
                int lastBracket = log.lastIndexOf("}");
                int firstBracket = log.indexOf("{");
                if (log.length() >= lastBracket) {
                    log = log.substring(Math.max(0, firstBracket), lastBracket + 1);
                }

                Matcher matcher = BETWEEN_JSON_OBJECTS_PATTERN.matcher(log);

                log = matcher.replaceAll("}, \\{");
                matcher = ALL_BEFORE_JSON_PATTERN.matcher(log);
                log = "[" + matcher.replaceFirst("{") + "]";

                try {
                    new JsonArray(log);
                    LOGGER.info("JSON format logging successfully set for {} - {}", podName, containerName);
                    isJSON = true;
                } catch (Exception e) {
                    LOGGER.info(log);
                    LOGGER.info("Failed to set JSON format logging for {} - {}", podName, containerName, e);
                    isJSON = false;
                    break;
                }
            }
            return isJSON;
        });
    }

    /**
     * Method for check if test is allowed on current Kubernetes version
     * @param maxKubernetesVersion kubernetes version which test needs
     * @return true if test is allowed, false if not
     */
    public static boolean isAllowedOnCurrentK8sVersion(String maxKubernetesVersion) {
        if (maxKubernetesVersion.equals("latest")) {
            return true;
        }
        return Double.parseDouble(kubeClient().clusterKubernetesVersion()) < Double.parseDouble(maxKubernetesVersion);
    }

    /**
     * Method which returns log from last {@code timeSince}
     * @param podName name of pod to take a log from
     * @param containerName name of container
     * @param timeSince time from which the log should be taken - 3s, 5m, 2h -- back
     * @return log from the pod
     */
    public static String getLogFromPodByTime(String podName, String containerName, String timeSince) {
        return cmdKubeClient().execInCurrentNamespace("logs", podName, "-c", containerName, "--since=" + timeSince).out();
    }

    /**
     * Change Deployment configuration before applying it. We set different namespace, log level and image pull policy.
     * It's mostly used for use cases where we use direct kubectl command instead of fabric8 calls to api.
     * @param deploymentFile loaded Strimzi deployment file
     * @param namespace namespace where Strimzi should be installed
     * @return deployment file content as String
     */
    public static String changeDeploymentNamespace(File deploymentFile, String namespace) {
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
                    ((ObjectNode) envVar).put("value", Environment.STRIMZI_LOG_LEVEL);
                }
            }
            // Change image pull policy
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode imagePulPolicyEnvVar = objectMapper.createObjectNode();
            imagePulPolicyEnvVar.put("name", "STRIMZI_IMAGE_PULL_POLICY");
            imagePulPolicyEnvVar.put("value", Environment.COMPONENTS_IMAGE_PULL_POLICY);
            ((ArrayNode) containerNode.get("env")).add(imagePulPolicyEnvVar);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

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
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.VersionInfo;
import io.skodjob.kubetest4j.enums.LogLevel;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.TestTags;
import io.strimzi.systemtest.annotations.SkipDefaultNetworkPolicyCreation;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StUtils {

    private static final Logger LOGGER = LogManager.getLogger(StUtils.class);

    private static final Pattern KAFKA_COMPONENT_PATTERN = Pattern.compile("([^-|^_]*?)(?<kafka>[-|_]kafka[-|_])(?<version>.*)$");

    public static final Pattern IMAGE_PATTERN_FULL_PATH = Pattern.compile(
        "^(?:(?<registry>[a-zA-Z0-9.-]+(?::\\d+)?)/)?" +              // Group for registry (and port)
        "(?<org>[a-z0-9][a-z0-9._-]*(?:/[a-z0-9._-]+)*)/" +           // Full repository path (org)
        "(?<image>[a-zA-Z0-9._-]+)" +                                 // Name of the image
        "(?::(?<tag>[a-zA-Z0-9._-]+))?$"                              // Tag of the image
    );

    public static final Pattern JSON_TEMPLATE_LAYOUT_PATTERN = Pattern.compile("\\{\"instant\":\"(?<date>[a-zA-Z0-9-:.]+)\",\"someConstant\":1,\"message\":\"(?<msg>.+)\"}");

    private static final Pattern IMAGE_PATTERN = Pattern.compile("^(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");

    private static final Pattern VERSION_IMAGE_PATTERN = Pattern.compile("(?<version>[0-9.]+)=(?<image>[^\\s]*)");

    private static final BiFunction<String, Object, Boolean> CONTAINS_ANNOTATION =
        (annotationName, annotationHolder) -> {
            Annotation[] annotations;
            if (annotationHolder instanceof ExtensionContext) {
                annotations = ((ExtensionContext) annotationHolder).getElement().get().getAnnotations();
            } else if (annotationHolder instanceof ClassDescriptor) {
                annotations = ((ClassDescriptor) annotationHolder).getTestClass().getAnnotations();
            } else {
                throw new RuntimeException("Using type: " + annotationHolder + " which is not supported!");
            }
            return Arrays.stream(annotations).filter(
                annotation -> annotation.annotationType().getName()
                    .toLowerCase(Locale.ENGLISH)
                    // more than one because in some cases the TestSuite can inherit the annotation
                    .contains(annotationName)).count() >= 1;

        };

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
        // in case that the image in the file contains `@sha256:`, we want to test exactly some digest and we
        // will skip configuring the other parts, so returning the image as is
        if (image.contains("@sha256:")) {
            return image;
        }

        Matcher m = IMAGE_PATTERN_FULL_PATH.matcher(image);
        if (m.find()) {
            String registry = setImageProperties(m.group("registry"), Environment.STRIMZI_REGISTRY);
            String org = setImageProperties(m.group("org"), Environment.STRIMZI_ORG);

            return registry + "/" + org + "/" + m.group("image") + ":" + buildTag(m.group("tag"));
        }
        m = IMAGE_PATTERN.matcher(image);
        if (m.find()) {
            String org = setImageProperties(m.group("org"), Environment.STRIMZI_ORG);

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

    private static String setImageProperties(String current, String envVar) {
        if (!envVar.isEmpty() && !current.equals(envVar)) {
            return envVar;
        }
        return current;
    }

    private static String buildTag(String currentTag) {
        if (!Environment.STRIMZI_TAG.isEmpty() && !currentTag.equals(Environment.STRIMZI_TAG)) {
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
        String[] allLines = config.split("\\r?\\n");
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
        jsonObject.put(TestConstants.TLS_LISTENER_DEFAULT_NAME, tls);
        jsonObject.put("protocol", protocol);
        jsonObject.put("auth", auth);

        JsonArray jsonArray = new JsonArray();
        jsonArray.add(jsonObject);

        return jsonArray;
    }

    /**
     * Build jsonArray with data about service discovery based on pass configuration
     * @param plainEncryption plain listener encryption
     * @param tlsEncryption tls listener encryption
     * @param plainTlsAuth plain listener authentication
     * @param tlsTlsAuth tls listener authentication
     * @return builded jsonArray
     */
    public static JsonArray expectedServiceDiscoveryInfo(String plainEncryption, String tlsEncryption, boolean plainTlsAuth, boolean tlsTlsAuth) {
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(expectedServiceDiscoveryInfo(9092, "kafka", plainEncryption, plainTlsAuth).getValue(0));
        jsonArray.add(expectedServiceDiscoveryInfo(9093, "kafka", tlsEncryption, tlsTlsAuth).getValue(0));
        return jsonArray;
    }

    /**
     * Method for checking if JSON format logging is set for the {@code Pods}
     * @param namespaceName Namespace name
     * @param pods snapshot of Pods to be checked
     * @param containerName name of container from which to take the log
     * @param regexPattern Optional regex pattern to match the log entries
     */
    public static void checkLogForJSONFormat(String namespaceName, Map<String, String> pods, String containerName, Pattern regexPattern) {
        TestUtils.waitFor("JSON log to be present in " + pods, TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT, () -> {
            boolean isValidJSON = false;
            for (String podName : pods.keySet()) {
                String log = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).exec(LogLevel.DEBUG, "logs", podName, "-c", containerName, "--tail=100").out();

                JsonArray jsonArray = getJsonArrayFromLog(log);

                // 2 is just in case we will take some JSON that is not part of the JSON format logging
                if (!jsonArray.isEmpty() && jsonArray.size() >= 2) {

                    isValidJSON = regexPattern == null || regexPattern.matcher(jsonArray.getString(0)).find();

                    if (isValidJSON) {
                        LOGGER.info("JSON format logging successfully set for Pod: {}", podName);
                    }
                }
            }
            return isValidJSON;
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
        return Double.parseDouble(getKubernetesClusterVersion()) < Double.parseDouble(maxKubernetesVersion);
    }

    /**
     * Method which return kubernetes version
     * @return kubernetes version
     */
    public static String getKubernetesClusterVersion() {
        VersionInfo versionInfo = KubeResourceManager.get().kubeClient().getClient().getKubernetesVersion();
        return versionInfo.getMajor() + "." + versionInfo.getMinor().replace("+", "");
    }

    /**
     * Method which returns log from last {@code timeSince}
     * @param namespaceName name of the namespace
     * @param podName name of pod to take a log from
     * @param containerName name of container
     * @param timeSince time from which the log should be taken - 3s, 5m, 2h -- back
     * @return log from the pod
     */
    public static String getLogFromPodByTime(String namespaceName, String podName, String containerName, String timeSince) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).exec(true, false, "logs", podName, "-c", containerName, "--since=" + timeSince).out();
    }

    /**
     * Dynamic waiting for specific string inside pod's log. In case pod's log doesn't contains {@code exceptedString}
     * it will caused WaitException.
     * @param namespaceName name of the Namespace where the logs are checked
     * @param podName name of the Pod
     * @param containerName name of container
     * @param timeSince time from which the log should be taken - 3s, 5m, 2h -- back
     * @param exceptedString log message to be checked
     */
    public static void waitUntilLogFromPodContainsString(String namespaceName, String podName, String containerName, String timeSince, String exceptedString) {
        TestUtils.waitFor("log from Pod: " + namespaceName + "/" + podName + " to contain string: " + exceptedString, TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> getLogFromPodByTime(namespaceName, podName, containerName, timeSince).contains(exceptedString));
    }

    /**
     * Change Deployment configuration before applying it. We set different namespace, log level and image pull policy.
     * It's mostly used for use cases where we use direct kubectl command instead of fabric8 calls to api.
     * @param deploymentFile loaded Strimzi deployment file
     * @param strimziFeatureGatesValue feature gates value
     * @return deployment file content as String
     */
    public static String changeDeploymentConfiguration(String namespaceName, File deploymentFile, final String strimziFeatureGatesValue) {
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
                    ((ObjectNode) envVar).put("value", namespaceName);
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

            if (strimziFeatureGatesValue != null && !strimziFeatureGatesValue.isEmpty()) {
                ObjectNode strimziFeatureGates =  new ObjectMapper().createObjectNode();
                strimziFeatureGates.put("name", "STRIMZI_FEATURE_GATES");
                strimziFeatureGates.put("value", strimziFeatureGatesValue);
                ((ArrayNode) containerNode.get("env")).add(strimziFeatureGates);
            }
            ((ArrayNode) containerNode.get("env")).add(imagePulPolicyEnvVar);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getLineFromPodContainer(String namespaceName, String podName, String containerName, String filePath, String grepString) {
        if (containerName == null) {
            return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(LogLevel.DEBUG, podName, "grep", "-i", grepString, filePath).out().trim();
        } else {
            return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPodContainer(LogLevel.DEBUG, podName, containerName, "grep", "-i", grepString, filePath).out().trim();
        }
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelTest}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelTest},
     * otherwise false
     */
    public static boolean isParallelTest(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(TestTags.PARALLEL_TEST, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.IsolatedTest}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.IsolatedTest},
     * otherwise false
     */
    public static boolean isIsolatedTest(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(TestTags.ISOLATED_TEST, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest},
     * otherwise false
     */
    public static boolean isParallelNamespaceTest(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(TestTags.PARALLEL_NAMESPACE, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.SkipDefaultNetworkPolicyCreation}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.SkipDefaultNetworkPolicyCreation},
     * otherwise false
     */
    public static boolean shouldSkipNetworkPoliciesCreation(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(SkipDefaultNetworkPolicyCreation.class.getName().toLowerCase(Locale.ROOT), annotationHolder);
    }

    /**
     * Retrieve namespace based on the cluster configuration
     *
     * @param namespaceName    suite namespace
     * @param extensionContext test context for get the parallel namespace
     * @return single or parallel namespace based on cluster configuration
     */
    public static String getNamespaceBasedOnRbac(String namespaceName, ExtensionContext extensionContext) {
        return Environment.isNamespaceRbacScope() ? namespaceName : extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.NAMESPACE_KEY).toString();
    }

    /**
     * Copies the image pull secret from the default namespace to the specified target namespace.
     *
     * @param namespaceName the target namespace
     */
    public static void copyImagePullSecrets(String namespaceName) {
        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            LOGGER.info("Checking if Secret: {} is in the default Namespace", Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);
            if (KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace("default").withName(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET).get() == null) {
                throw new RuntimeException(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET + " is not in the default Namespace!");
            }
            LOGGER.info("Creating pull Secret: {}/{}", namespaceName, Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);
            Secret pullSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace("default").withName(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET).get();
            KubeResourceManager.get().createResourceWithWait(new SecretBuilder()
                .withApiVersion("v1")
                .withKind("Secret")
                .withNewMetadata()
                    .withName(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withType("kubernetes.io/dockerconfigjson")
                .withData(Collections.singletonMap(".dockerconfigjson", pullSecret.getData().get(".dockerconfigjson")))
                .build());
        }
        if (Environment.CONNECT_BUILD_REGISTRY_SECRET != null && !Environment.CONNECT_BUILD_REGISTRY_SECRET.isEmpty()) {
            LOGGER.info("Checking if Secret: {} is in the default Namespace", Environment.CONNECT_BUILD_REGISTRY_SECRET);
            if (KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace("default").withName(Environment.CONNECT_BUILD_REGISTRY_SECRET).get() == null) {
                throw new RuntimeException(Environment.CONNECT_BUILD_REGISTRY_SECRET + " is not in the default namespace!");
            }
            LOGGER.info("Creating pull Secret: {}/{}", namespaceName, Environment.CONNECT_BUILD_REGISTRY_SECRET);
            Secret pullSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace("default").withName(Environment.CONNECT_BUILD_REGISTRY_SECRET).get();
            KubeResourceManager.get().createResourceWithWait(new SecretBuilder()
                .withApiVersion("v1")
                .withKind("Secret")
                .withNewMetadata()
                    .withName(Environment.CONNECT_BUILD_REGISTRY_SECRET)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withType("kubernetes.io/dockerconfigjson")
                .withData(Collections.singletonMap(".dockerconfigjson", pullSecret.getData().get(".dockerconfigjson")))
                .build());
        }
    }

    /**
     * Parses JsonObjects from pod log, which can contains also normal (not JSON formatted) text
     * The parses looks for occurrences of \{ and \}, saving all characters between to {@code temp}
     * After \} is detected, JsonObject from {@code temp} is created, {@code temp} and {@code stack}
     * are cleared.
     * @param log - log from pod containing JSON objects/arrays
     * @return JsonArray with objects found in {@param log}
     */
    private static JsonArray getJsonArrayFromLog(String log) {
        List<Character> stack = new ArrayList<>();
        List<JsonObject> jsonObjects = new ArrayList<>();
        StringBuilder temp = new StringBuilder();

        for (char eachChar: log.toCharArray()) {
            if (stack.isEmpty() && eachChar == '{') {
                stack.add(eachChar);
                temp.append(eachChar);
            } else if (!stack.isEmpty()) {
                temp.append(eachChar);

                if (stack.get(stack.size() - 1).equals('{') && eachChar == '}') {
                    stack.remove(stack.size() - 1);

                    if (stack.isEmpty()) {
                        JsonObject newObject = getJsonObjectFromString(temp.toString());
                        if (!newObject.isEmpty()) {
                            jsonObjects.add(newObject);
                        }
                        temp = new StringBuilder();
                    }
                } else if (eachChar == '{' || eachChar == '}') {
                    stack.add(eachChar);
                }
            } else if (temp.length() > 0) {
                JsonObject newObject = getJsonObjectFromString(temp.toString());
                if (!newObject.isEmpty()) {
                    jsonObjects.add(newObject);
                }
                temp = new StringBuilder();
            }
        }

        return new JsonArray(jsonObjects);
    }

    private static JsonObject getJsonObjectFromString(String object) {
        try {
            return new JsonObject(object);
        } catch (DecodeException e) {
            return new JsonObject("{}");
        }
    }

    public static String removePackageName(String testClassPath) {
        return testClassPath.replace("io.strimzi.systemtest.", "");
    }

    public static Affinity getDeploymentOrStrimziPodSetAffinity(String namespaceName, String resourceName) {
        Pod firstPod = StrimziPodSetUtils.getFirstPodFromSpec(namespaceName, resourceName);
        return firstPod.getSpec().getAffinity();
    }

    public static void waitTillStrimziPodSetOrDeploymentRolled(final String namespaceName, final String depName,
                                                               final int expectPods, final Map<String, String> snapShot,
                                                               final LabelSelector labelSelector) {
        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, labelSelector, snapShot);
        RollingUpdateUtils.waitForComponentAndPodsReady(namespaceName, labelSelector, expectPods);
    }

    /**
     * Returns a list of names of ConfigMaps with broker configuration files.
     * For StrimziPodSets, it should be a ConfigMap per broker.
     *
     * @param namespaceName  Name of the Namespace where is the Kafka cluster running
     * @param kafkaClusterName  Name of the Kafka cluster
     *
     * @return                  List with ConfigMaps containing the configuration
     */
    public static List<String> getKafkaConfigurationConfigMaps(String namespaceName, String kafkaClusterName) {
        return PodUtils.listPodNames(namespaceName, LabelSelectors.kafkaLabelSelector(kafkaClusterName, KafkaComponents.getBrokerPodSetName(kafkaClusterName)));
    }

    public static void waitUntilSuppliersAreMatching(final Supplier<?> sup, final Supplier<?> anotherSup) {
        TestUtils.waitFor(sup.get() + " is matching with" + anotherSup.get(), TestConstants.GLOBAL_POLL_INTERVAL,
                TestConstants.GLOBAL_STATUS_TIMEOUT, () -> sup.get().equals(anotherSup.get()));
    }

    public static void waitUntilSupplierIsSatisfied(String message, final BooleanSupplier sup) {
        TestUtils.waitFor(message, TestConstants.GLOBAL_POLL_INTERVAL,
                TestConstants.GLOBAL_STATUS_TIMEOUT, sup);
    }

    /**
     * Indents the input string with for empty spaces at the beginning of each line.
     *
     * @param input     Input string that should be indented
     *
     * @return  Indented string
     */
    public static String indent(String input) {
        StringBuilder sb = new StringBuilder();
        String[] lines = input.split("[\n\r]");

        for (String line : lines) {
            sb.append("    ").append(line).append(System.lineSeparator());
        }

        return sb.toString();
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

    /**
     * Parse the image map String into a Map.
     *
     * @param imageMapString    String with the image map (contains key-value pairs separated by new lines in a single string)
     *
     * @return  Map with the parsed images
     */
    public static Map<String, String> parseImageMap(String imageMapString) {
        if (imageMapString != null) {
            StringTokenizer tok = new StringTokenizer(imageMapString, ", \t\n\r");
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
     * Method for cutting the length of the test case in case that it's too long for having it as label in the particular resource.
     *
     * @param testCaseName  test case name that should be trimmed
     *
     * @return  trimmed test case name if needed
     */
    public static String trimTestCaseBaseOnItsLength(String testCaseName) {
        // because label values `must be no more than 63 characters`
        if (testCaseName.length() > 63) {
            // we cut to 62 characters
            return testCaseName.substring(0, 62);
        }

        return testCaseName;
    }

    /**
     * Lists events in Namespace by specific resource UID.
     *
     * @param namespaceName     Name of Namespace where the events should be listed.
     * @param resourceUid       UID of resource for which the events should be listed.
     *
     * @return  events in Namespace by specific resource UID.
     */
    public static List<Event> listEventsByResourceUid(String namespaceName, String resourceUid) {
        return KubeResourceManager.get().kubeClient().getClient().v1().events().inNamespace(namespaceName).list().getItems().stream()
            .filter(event -> {
                if (event.getInvolvedObject().getUid() == null) {
                    return false;
                }
                return event.getInvolvedObject().getUid().equals(resourceUid);
            })
            .collect(Collectors.toList());
    }
}

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
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
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

    public static String checkEnvVarInPod(String namespaceName, String podName, String envVarName) {
        return kubeClient(namespaceName).getPod(podName).getSpec().getContainers().get(0).getEnv()
                .stream().filter(envVar -> envVar.getName().equals(envVarName)).findFirst().orElseThrow().getValue();
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
     * @param namespaceName Namespace name
     * @param pods snapshot of pods to be checked
     * @param containerName name of container from which to take the log
     */
    public static void checkLogForJSONFormat(String namespaceName, Map<String, String> pods, String containerName) {
        //this is only for decrease the number of records - kafka have record/line, operators record/11lines
        String tail = "--tail=" + (containerName.contains("operator") ? "100" : "10");

        TestUtils.waitFor("for JSON log in " + pods, Constants.GLOBAL_POLL_INTERVAL_MEDIUM, Constants.GLOBAL_TIMEOUT, () -> {
            boolean isJSON = false;
            for (String podName : pods.keySet()) {
                String log = cmdKubeClient().namespace(namespaceName).execInCurrentNamespace(Level.TRACE, "logs", podName, "-c", containerName, tail).out();

                JsonArray jsonArray = getJsonArrayFromLog(log);

                // 2 is just in case we will take some JSON that is not part of the JSON format logging
                if (!jsonArray.isEmpty() && jsonArray.size() >= 2) {
                    LOGGER.info("JSON format logging successfully set for pod: {}", podName);
                    isJSON = true;
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
     * @param namespaceName name of the namespace
     * @param podName name of pod to take a log from
     * @param containerName name of container
     * @param timeSince time from which the log should be taken - 3s, 5m, 2h -- back
     * @return log from the pod
     */
    public static String getLogFromPodByTime(String namespaceName, String podName, String containerName, String timeSince) {
        return cmdKubeClient().namespace(namespaceName).execInCurrentNamespace("logs", podName, "-c", containerName, "--since=" + timeSince).out();
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
        TestUtils.waitFor("log from pod contains excepted string:" + exceptedString, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> getLogFromPodByTime(namespaceName, podName, containerName, timeSince).contains(exceptedString));
    }

    /**
     * Change Deployment configuration before applying it. We set different namespace, log level and image pull policy.
     * It's mostly used for use cases where we use direct kubectl command instead of fabric8 calls to api.
     * @param deploymentFile loaded Strimzi deployment file
     * @param namespace namespace where Strimzi should be installed
     * @param strimziFeatureGatesValue feature gates value
     * @return deployment file content as String
     */
    public static String changeDeploymentConfiguration(File deploymentFile, String namespace, final String strimziFeatureGatesValue) {
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
            return KubeClusterResource.cmdKubeClient(namespaceName).execInPod(podName, "grep", "-i", grepString, filePath).out().trim();
        } else {
            return KubeClusterResource.cmdKubeClient(namespaceName).execInPodContainer(podName, containerName, "grep", "-i", grepString, filePath).out().trim();
        }
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelTest}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelTest},
     * otherwise false
     */
    public static boolean isParallelTest(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(Constants.PARALLEL_TEST, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.IsolatedTest}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.IsolatedTest},
     * otherwise false
     */
    public static boolean isIsolatedTest(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(Constants.ISOLATED_TEST, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest},
     * otherwise false
     */
    public static boolean isParallelNamespaceTest(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(Constants.PARALLEL_NAMESPACE, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelSuite}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.ParallelSuite},
     * otherwise false
     */
    public static boolean isParallelSuite(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(Constants.PARALLEL_SUITE, annotationHolder);
    }

    /**
     * Checking if test case contains annotation {@link io.strimzi.systemtest.annotations.IsolatedSuite}
     * @param annotationHolder context of the test case
     * @return true if test case contains annotation {@link io.strimzi.systemtest.annotations.IsolatedSuite},
     * otherwise false
     */
    public static boolean isIsolatedSuite(Object annotationHolder) {
        return CONTAINS_ANNOTATION.apply(Constants.ISOLATED_SUITE, annotationHolder);
    }

    /**
     * Retrieve namespace based on the cluster configuration
     * @param namespace suite namespace
     * @param extensionContext test context for get the parallel namespace
     * @return single or parallel namespace based on cluster configuration
     */
    public static String getNamespaceBasedOnRbac(String namespace, ExtensionContext extensionContext) {
        return Environment.isNamespaceRbacScope() ? namespace : extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
    }

    /**
     * Copies the image pull secret from the default namespace to the specified target namespace.
     * @param namespace the target namespace
     */
    public static void copyImagePullSecret(String namespace) {
        LOGGER.info("Checking if secret {} is in the default namespace", Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);
        if (kubeClient("default").getSecret(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET) == null) {
            throw new RuntimeException(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET + " is not in the default namespace!");
        }
        Secret pullSecret = kubeClient("default").getSecret(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);
        kubeClient(namespace).createSecret(new SecretBuilder()
                .withApiVersion("v1")
                .withKind("Secret")
                .withNewMetadata()
                    .withName(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET)
                    .withNamespace(namespace)
                .endMetadata()
                .withType("kubernetes.io/dockerconfigjson")
                .withData(Collections.singletonMap(".dockerconfigjson", pullSecret.getData().get(".dockerconfigjson")))
                .build());
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

    /**
     * These methods are operating with StatefulSets or StrimziPodSets depending on ENV variable.
     * They should be removed together with StatefulSets in the future - and we should use
     * StrimziPodSets related methods instead.
     */

    public static void annotateStatefulSetOrStrimziPodSet(String namespaceName, String resourceName, Map<String, String> annotations) {
        if (Environment.isStrimziPodSetEnabled()) {
            StrimziPodSetResource.replaceStrimziPodSetInSpecificNamespace(resourceName,
                strimziPodSet -> strimziPodSet.getMetadata().setAnnotations(annotations), namespaceName);
        } else {
            kubeClient(namespaceName)
                .statefulSet(resourceName)
                .edit(sts -> new StatefulSetBuilder(sts)
                .editMetadata()
                    .addToAnnotations(annotations)
                .endMetadata()
                .build());
        }
    }

    public static Map<String, String> getAnnotationsOfStatefulSetOrStrimziPodSet(String namespaceName, String resourceName) {
        if (Environment.isStrimziPodSetEnabled()) {
            return StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getAnnotations();
        }
        return kubeClient().getStatefulSet(namespaceName, resourceName).getMetadata().getAnnotations();
    }

    public static Map<String, String> getLabelsOfStatefulSetOrStrimziPodSet(String namespaceName, String resourceName) {
        if (Environment.isStrimziPodSetEnabled()) {
            return StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels();
        }
        return kubeClient().getStatefulSet(namespaceName, resourceName).getMetadata().getLabels();
    }

    public static void waitForStatefulSetOrStrimziPodSetLabelsChange(String namespaceName, String resourceName, Map<String, String> labels) {
        if (Environment.isStrimziPodSetEnabled()) {
            StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(namespaceName, resourceName, labels);
        } else {
            StatefulSetUtils.waitForStatefulSetLabelsChange(namespaceName, resourceName, labels);
        }
    }

    public static void waitForStatefulSetOrStrimziPodSetLabelsDeletion(String namespaceName, String resourceName, String... labelKeys) {
        if (Environment.isStrimziPodSetEnabled()) {
            StrimziPodSetUtils.waitForStrimziPodSetLabelsDeletion(namespaceName, resourceName, labelKeys);
        } else {
            StatefulSetUtils.waitForStatefulSetLabelsDeletion(namespaceName, resourceName, labelKeys);
        }
    }

    public static Affinity getStatefulSetOrStrimziPodSetAffinity(String namespaceName, String resourceName) {
        if (Environment.isStrimziPodSetEnabled()) {
            Pod firstPod = StrimziPodSetUtils.getFirstPodFromSpec(namespaceName, resourceName);
            return firstPod.getSpec().getAffinity();
        } else {
            return kubeClient().getStatefulSet(namespaceName, resourceName).getSpec().getTemplate().getSpec().getAffinity();
        }
    }

    public static void deleteStrimziPodSetOrStatefulSet(String namespaceName, String resourceName) {
        if (Environment.isStrimziPodSetEnabled()) {
            StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).delete();
        } else {
            kubeClient(namespaceName).deleteStatefulSet(resourceName);
        }
    }

    public static void waitForStrimziPodSetOrStatefulSetRecovery(String namespaceName, String resourceName, String resourceUID) {
        if (Environment.isStrimziPodSetEnabled()) {
            StrimziPodSetUtils.waitForStrimziPodSetRecovery(namespaceName, resourceName, resourceUID);
        } else {
            StatefulSetUtils.waitForStatefulSetRecovery(namespaceName, resourceName, resourceUID);
        }
    }

    public static void waitForStrimziPodSetOrStatefulSetAndPodsReady(String namespaceName, String resourceName, int expectPods) {
        if (Environment.isStrimziPodSetEnabled()) {
            StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(namespaceName, resourceName, expectPods);
        } else {
            StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, resourceName, expectPods);
        }
    }

    public static String getStrimziPodSetOrStatefulSetUID(String namespaceName, String resourceName) {
        if (Environment.isStrimziPodSetEnabled()) {
            return StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getUid();
        }
        return kubeClient(namespaceName).getStatefulSetUid(resourceName);
    }

    /**
     * Returns a list of names of ConfigMaps with broker configuration files. For StatefulSets, the list has only one
     * item with the shared ConfigMap. For StrimziPodSets, it should be a ConfigMap per broker.
     *
     * @param kafkaClusterName  Name of the Kafka cluster
     * @param replicas          Number of Kafka replicas
     *
     * @return                  List with ConfigMaps containing the configuration
     */
    public static List<String> getKafkaConfigurationConfigMaps(String kafkaClusterName, int replicas)    {
        List<String> cmNames = new ArrayList<>(replicas);

        if (Environment.isStrimziPodSetEnabled())   {
            for (int i = 0; i < replicas; i++)  {
                cmNames.add(KafkaResources.kafkaPodName(kafkaClusterName, i));
            }
        } else {
            cmNames.add(KafkaResources.kafkaMetricsAndLogConfigMapName(kafkaClusterName));
        }

        return cmNames;
    }
    public static void waitUntilSuppliersAreMatching(final Supplier<?> sup, final Supplier<?> anotherSup) {
        TestUtils.waitFor(sup.get() + " is matching with" + anotherSup.get(), Constants.GLOBAL_POLL_INTERVAL,
                Constants.GLOBAL_STATUS_TIMEOUT, () -> sup.get().equals(anotherSup.get()));
    }

    public static void waitUntilSupplierIsSatisfied(final BooleanSupplier sup) {
        TestUtils.waitFor(sup.getAsBoolean() + " is satisfied", Constants.GLOBAL_POLL_INTERVAL,
                Constants.GLOBAL_STATUS_TIMEOUT, sup);
    }
}
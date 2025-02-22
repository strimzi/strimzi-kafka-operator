/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokers;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.kafka.KafkaClusterSpec.FORBIDDEN_PREFIXES;
import static io.strimzi.api.kafka.model.kafka.KafkaClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();
    private static final Random RANDOM = new Random();

    private KafkaUtils() {}

    public static boolean waitForKafkaReady(String namespaceName, String clusterName) {
        return waitForKafkaStatus(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaNotReady(String namespaceName, String clusterName) {
        return waitForKafkaStatus(namespaceName, clusterName, NotReady);
    }

    public static boolean waitForKafkaStatus(String namespaceName, String clusterName, Enum<?>  state) {
        Kafka kafka = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaResource.kafkaClient(), kafka, state);
    }

    /**
     * Waits for the Kafka Status to be updated after changed. It checks the generation and observed generation to
     * ensure the status is up to date.
     *
     * @param namespaceName Namespace name
     * @param clusterName   Name of the Kafka cluster which should be checked
     */
    public static void waitForKafkaStatusUpdate(String namespaceName, String clusterName) {
        LOGGER.info("Waiting for Kafka status to be updated");
        TestUtils.waitFor("Kafka status to be updated", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            Kafka k = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
            return k.getMetadata().getGeneration() == k.getStatus().getObservedGeneration();
        });
    }

    public static void waitUntilKafkaStatusConditionContainsMessage(String namespaceName, String clusterName, String pattern, long timeout) {
        TestUtils.waitFor("Kafka status to contain message [" + pattern + "]",
            TestConstants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                List<Condition> conditions = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getConditions();
                for (Condition condition : conditions) {
                    String conditionMessage = condition.getMessage();
                    if (conditionMessage != null && conditionMessage.matches(pattern)) {
                        return true;
                    }
                }
                return false;
            });
    }

    public static void waitUntilStatusKafkaVersionMatchesExpectedVersion(String namespaceName, String clusterName, String expectedKafkaVersion) {
        TestUtils.waitFor("Kafka version '" + expectedKafkaVersion + "' in Kafka cluster '" + clusterName + "' to match",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
                String kafkaVersionInStatus = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getKafkaVersion();
                return expectedKafkaVersion.equals(kafkaVersionInStatus);
            });
    }

    public static void waitUntilKafkaStatusConditionContainsMessage(String namespaceName, String clusterName, String pattern) {
        waitUntilKafkaStatusConditionContainsMessage(namespaceName, clusterName, pattern, TestConstants.GLOBAL_STATUS_TIMEOUT);
    }

    public static String getKafkaStatusCertificates(String namespaceName, String listenerType, String clusterName) {
        String certs = "";
        List<ListenerStatus> kafkaListeners = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getListeners();

        for (ListenerStatus listener : kafkaListeners) {
            if (listener.getName().equals(listenerType))
                certs = listener.getCertificates().toString();
        }
        certs = certs.substring(1, certs.length() - 1);
        return certs;
    }

    public static String getKafkaSecretCertificates(String namespaceName, String secretName, String certType) {
        String secretCerts = "";
        secretCerts = kubeClient(namespaceName).getSecret(namespaceName, secretName).getData().get(certType);
        return Util.decodeFromBase64(secretCerts, Charset.defaultCharset());
    }

    public static void waitForKafkaSecretAndStatusCertsMatches(Supplier<String> kafkaStatusCertificate, Supplier<String> kafkaSecretCertificate) {
        TestUtils.waitFor("Kafka Secret and Kafka status certificates to match", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> kafkaStatusCertificate.get().equals(kafkaSecretCertificate.get()));
    }

    @SuppressWarnings("unchecked")
    public static void waitForClusterStability(String namespaceName, String clusterName) {
        LabelSelector brokerSelector = KafkaResource.getLabelSelector(clusterName, StrimziPodSetResource.getBrokerComponentName(clusterName));
        LabelSelector controllerSelector = KafkaResource.getLabelSelector(clusterName, StrimziPodSetResource.getControllerComponentName(clusterName));

        Map<String, String>[] controllerPods = new Map[1];
        Map<String, String>[] brokerPods = new Map[1];
        Map<String, String>[] eoPods = new Map[1];

        LOGGER.info("Waiting for cluster stability");

        int[] count = {0};

        brokerPods[0] = PodUtils.podSnapshot(namespaceName, brokerSelector);
        controllerPods[0] = PodUtils.podSnapshot(namespaceName, controllerSelector);
        eoPods[0] = DeploymentUtils.depSnapshot(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));

        TestUtils.waitFor("Cluster to be stable and ready", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Map<String, String> brokerSnapshot = PodUtils.podSnapshot(namespaceName, brokerSelector);
            Map<String, String> controllerSnapshot = PodUtils.podSnapshot(namespaceName, controllerSelector);
            Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));

            boolean brokersSameAsLast = brokerSnapshot.equals(brokerPods[0]);
            boolean controllersSameAsLast = controllerSnapshot.equals(controllerPods[0]);
            boolean eoSameAsLast = eoSnapshot.equals(eoPods[0]);

            if (!brokersSameAsLast) {
                LOGGER.warn("Broker Pods are not stable");
            }
            if (!controllersSameAsLast) {
                LOGGER.warn("Controller Pods are not stable");
            }
            if (!eoSameAsLast) {
                LOGGER.warn("EO not stable");
            }

            if (brokersSameAsLast && controllersSameAsLast && eoSameAsLast) {
                int c = count[0]++;
                LOGGER.debug("All stable after: {} polls", c);
                if (c > 60) {
                    LOGGER.info("Kafka cluster is stable after: {} polls", c);
                    return true;
                }
                return false;
            }

            brokerPods[0] = brokerSnapshot;
            controllerPods[0] = controllerSnapshot;
            eoPods[0] = eoSnapshot;

            count[0] = 0;
            return false;
        });
    }

    /**
     * Method which, update/replace Kafka configuration
     * @param namespaceName name of the namespace
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static void updateSpecificConfiguration(final String namespaceName, String clusterName, String brokerConfigName, Object value) {
        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceName, clusterName, kafka -> {
            LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
            config.put(brokerConfigName, value);
            kafka.getSpec().getKafka().setConfig(config);
            LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
        });
    }

    /**
     * Method which, extends the @link updateConfiguration(String clusterName, KafkaConfiguration kafkaConfiguration, Object value) method
     * with stability and ensures after update of Kafka resource there will be not rolling update
     * @param namespaceName Namespace name
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static void  updateConfigurationWithStabilityWait(final String namespaceName, String clusterName, String brokerConfigName, Object value) {
        updateSpecificConfiguration(namespaceName, clusterName, brokerConfigName, value);
        waitForClusterStability(namespaceName, clusterName);
    }

    /**
     * Verifies that updated configuration was successfully changed inside Kafka CR
     * @param namespaceName name of the namespace
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public synchronized static boolean verifyCrDynamicConfiguration(final String namespaceName, String clusterName, String brokerConfigName, Object value) {
        LOGGER.info("Dynamic Configuration in Kafka CR is {}={} and expected is {}={}",
            brokerConfigName,
            KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getKafka().getConfig().get(brokerConfigName),
            brokerConfigName,
            value);

        return KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getKafka().getConfig().get(brokerConfigName).equals(value);
    }

    /**
     * Verifies that updated configuration was successfully changed inside Kafka pods
     * @param namespaceName name of the namespace
     * @param kafkaPodNamePrefix prefix of Kafka pods
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     * @return
     * true = if specific property match the excepted property
     * false = if specific property doesn't match the excepted property
     */
    public synchronized static boolean verifyPodDynamicConfiguration(final String namespaceName, String scraperPodName, String bootstrapServer, String kafkaPodNamePrefix, String brokerConfigName, Object value) {

        List<Pod> brokerPods = kubeClient().listPodsByPrefixInName(namespaceName, kafkaPodNamePrefix);
        int[] brokerId = {0};

        for (Pod pod : brokerPods) {

            TestUtils.waitFor("dyn.configuration to change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.RECONCILIATION_INTERVAL + Duration.ofSeconds(10).toMillis(),
                () -> {
                    String result = KafkaCmdClient.describeKafkaBrokerUsingPodCli(namespaceName, scraperPodName, bootstrapServer, brokerId[0]++);

                    LOGGER.debug("This dyn.configuration {} inside the Kafka Pod: {}/{}", result, namespaceName, pod.getMetadata().getName());

                    if (!result.contains(brokerConfigName + "=" + value)) {
                        LOGGER.error("Kafka Pod: {}/{} doesn't contain {} with value {}", namespaceName, pod.getMetadata().getName(), brokerConfigName, value);
                        LOGGER.error("Kafka configuration {}", result);
                        return false;
                    }
                    return true;
                });
        }
        return true;
    }

    /**
     * Loads all kafka config parameters supported by the given {@code kafkaVersion}, as generated by #KafkaConfigModelGenerator in config-model-generator.
     * @param kafkaVersion specific kafka version
     * @return all supported kafka properties
     */
    public static Map<String, ConfigModel> readConfigModel(String kafkaVersion) {
        String name = TestUtils.USER_PATH + "/../cluster-operator/src/main/resources/kafka-" + kafkaVersion + "-config-model.json";
        try {
            try (InputStream in = new FileInputStream(name)) {
                ConfigModels configModels = new ObjectMapper().readValue(in, ConfigModels.class);
                if (!kafkaVersion.equals(configModels.getVersion())) {
                    throw new RuntimeException("Incorrect version");
                }
                return configModels.getConfigs();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from classpath resource " + name, e);
        }
    }

    /**
     * Return dynamic Kafka configs supported by the the given version of Kafka.
     * @param kafkaVersion specific kafka version
     * @return all dynamic properties for specific kafka version
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:BooleanExpressionComplexity", "unchecked"})
    public static Map<String, ConfigModel> getDynamicConfigurationProperties(String kafkaVersion)  {

        Map<String, ConfigModel> configs = KafkaUtils.readConfigModel(kafkaVersion);

        LOGGER.info("Kafka config {}", configs.toString());

        LOGGER.info("Number of all Kafka configs {}", configs.size());

        Map<String, ConfigModel> dynamicConfigs = configs
            .entrySet()
            .stream()
            .filter(a -> {
                String[] prefixKey = a.getKey().split("\\.");

                // filter all which is Scope = ClusterWide or PerBroker
                boolean isClusterWideOrPerBroker = a.getValue().getScope() == Scope.CLUSTER_WIDE || a.getValue().getScope() == Scope.PER_BROKER;

                if (prefixKey[0].equals("ssl") || prefixKey[0].equals("sasl") || prefixKey[0].equals("advertised") ||
                    prefixKey[0].equals("listeners") || prefixKey[0].equals("listener")) {
                    return isClusterWideOrPerBroker && !FORBIDDEN_PREFIXES.contains(prefixKey[0]);
                }

                return isClusterWideOrPerBroker;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        LOGGER.info("Number of dynamic-configs {}", dynamicConfigs.size());

        Map<String, ConfigModel> forbiddenExceptionsConfigs = configs
            .entrySet()
            .stream()
            .filter(a -> FORBIDDEN_PREFIX_EXCEPTIONS.contains(a.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        LOGGER.info("Number of forbidden-exception-configs {}", forbiddenExceptionsConfigs.size());

        Map<String, ConfigModel> dynamicConfigsWithExceptions = new HashMap<>();

        dynamicConfigsWithExceptions.putAll(dynamicConfigs);
        dynamicConfigsWithExceptions.putAll(forbiddenExceptionsConfigs);

        LOGGER.info("Size of dynamic-configs with forbidden-exception-configs {}", dynamicConfigsWithExceptions.size());

        dynamicConfigsWithExceptions.forEach((key, value) -> LOGGER.info(key + " -> "  + value.getScope() + ":" + value.getType()));

        return dynamicConfigsWithExceptions;
    }

    /**
     * Generated random name for the Kafka resource based on prefix
     * @param clusterName name prefix
     * @return name with prefix and random salt
     */
    public static String generateRandomNameOfKafka(String clusterName) {
        return clusterName + "-" + RANDOM.nextInt(Integer.MAX_VALUE);
    }

    public static String getVersionFromKafkaPodLibs(String namespaceName, String kafkaPodName) {
        String command = "ls libs | grep -Po 'kafka_\\d+.\\d+-\\K(\\d+.\\d+.\\d+)(?=.*jar)' | head -1 | cut -d \"-\" -f2";
        return cmdKubeClient(namespaceName).execInPodContainer(
            kafkaPodName,
            "kafka",
            "/bin/bash",
            "-c",
            command
        ).out().trim();
    }

    public static void waitForKafkaDeletion(String namespaceName, String kafkaClusterName) {
        LOGGER.info("Waiting for deletion of Kafka: {}/{}", namespaceName, kafkaClusterName);
        TestUtils.waitFor("deletion of Kafka: " + namespaceName + "/" + kafkaClusterName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaResource.kafkaClient().inNamespace(namespaceName).withName(kafkaClusterName).get() == null &&
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(StrimziPodSetResource.getControllerComponentName(kafkaClusterName)).get() == null  &&
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(StrimziPodSetResource.getBrokerComponentName(kafkaClusterName)).get() == null  &&
                    kubeClient(namespaceName).getDeployment(namespaceName, KafkaResources.entityOperatorDeploymentName(kafkaClusterName)) == null) {
                    return true;
                } else {
                    cmdKubeClient(namespaceName).deleteByName(Kafka.RESOURCE_KIND, kafkaClusterName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(kafkaClusterName).get()));
    }

    public static String getKafkaExternalListenerCaCertName(String namespaceName, String clusterName, String listenerName) {
        List<GenericKafkaListener> listeners = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getKafka().getListeners();

        GenericKafkaListener external = listenerName == null || listenerName.isEmpty() ?
            listeners.stream().filter(listener -> TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new) :
            listeners.stream().filter(listener -> listenerName.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new);

        if (external.getConfiguration() == null) {
            return KafkaResources.clusterCaCertificateSecretName(clusterName);
        } else {
            if (external.getConfiguration().getBrokerCertChainAndKey() != null) {
                return external.getConfiguration().getBrokerCertChainAndKey().getSecretName();
            } else {
                return KafkaResources.clusterCaCertificateSecretName(clusterName);
            }
        }
    }

    public static String changeOrRemoveKafkaInKRaft(File file, String version) {
        return changeOrRemoveKafkaConfigurationInKRaft(file, version, null);
    }

    public static String changeOrRemoveKafkaConfigurationInKRaft(File file, String version, String metadataVersion) {
        YAMLFactory yamlFactory = new YAMLFactory();
        ObjectMapper mapper = new ObjectMapper();
        YAMLMapper yamlMapper = new YAMLMapper();

        try {
            YAMLParser yamlParser = yamlFactory.createParser(file);
            List<ObjectNode> objects = mapper.readValues(yamlParser, new TypeReference<ObjectNode>() { }).readAll();

            ObjectNode kafkaResourceNode = objects.get(2);
            ObjectNode kafkaNode = (ObjectNode) kafkaResourceNode.at("/spec/kafka");

            ObjectNode entity = (ObjectNode) kafkaResourceNode.at("/spec/entityOperator");
            entity.set("topicOperator", mapper.createObjectNode());

            // workaround for current Strimzi upgrade (before we will have release containing metadataVersion in examples + CRDs)
            boolean metadataVersionFieldSupported = !cmdKubeClient().exec(false, "explain", "kafka.spec.kafka.metadataVersion").err().contains("does not exist");

            if (version == null) {
                kafkaNode.remove("version");
                kafkaNode.remove("metadataVersion");
            } else if (!version.equals("")) {
                kafkaNode.put("version", version);

                if (metadataVersionFieldSupported) {
                    kafkaNode.put("metadataVersion", TestKafkaVersion.getSpecificVersion(version).metadataVersion());
                }
            }

            if (metadataVersion != null && metadataVersionFieldSupported) {
                kafkaNode.put("metadataVersion", metadataVersion);
            }

            StringBuilder output = new StringBuilder();

            for (ObjectNode objectNode : objects) {
                output.append(yamlMapper.writeValueAsString(objectNode));
            }

            return output.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String namespacedPlainBootstrapAddress(String namespaceName, String clusterName) {
        return namespacedBootstrapAddress(namespaceName, clusterName, 9092);
    }

    public static String namespacedTlsBootstrapAddress(String namespaceName, String clusterName) {
        return namespacedBootstrapAddress(namespaceName, clusterName, 9093);
    }

    private static String namespacedBootstrapAddress(String namespaceName, String clusterName, int port) {
        return KafkaResources.bootstrapServiceName(clusterName) + "." + namespaceName + ".svc:" + port;
    }


    public static String bootstrapAddressFromStatus(String namespaceName, String clusterName, String listenerName) {

        List<ListenerStatus> listenerStatusList = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getListeners();

        if (listenerStatusList == null || listenerStatusList.size() < 1) {
            LOGGER.error("There is no Kafka external listener specified in the Kafka CR Status");
            throw new RuntimeException("There is no Kafka external listener specified in the Kafka CR Status");
        } else if (listenerName == null) {
            LOGGER.info("Listener name is not specified. Picking the first one from the Kafka Status");
            return listenerStatusList.get(0).getBootstrapServers();
        }

        return listenerStatusList.stream().filter(listener -> listener.getName().equals(listenerName))
                .findFirst()
                .orElseThrow(RuntimeException::new)
                .getBootstrapServers();
    }

    public static void annotateKafka(String namespaceName, String clusterName, Map<String, String> annotations) {
        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceName, clusterName, kafka -> kafka.getMetadata().getAnnotations().putAll(annotations));
    }

    public static void removeAnnotation(String namespaceName, String clusterName, String annotationKey) {
        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceName, clusterName, kafka -> kafka.getMetadata().getAnnotations().remove(annotationKey));
    }

    public static void waitUntilKafkaStatusContainsKafkaMetadataState(String namespaceName, String clusterName, KafkaMetadataState desiredKafkaMetadataState) {
        TestUtils.waitFor(String.join("Kafka status to be contain kafkaMetadataState: %s", desiredKafkaMetadataState.name()), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT, () -> {
            Kafka k = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
            return k.getStatus().getKafkaMetadataState().equals(desiredKafkaMetadataState);
        });
    }

    public static String getKafkaLogFolderNameInPod(String namespaceName, String kafkaPodName, String dataFolderName) {
        return ResourceManager.cmdKubeClient().namespace(namespaceName)
            .execInPod(kafkaPodName, "/bin/bash", "-c", "ls /var/lib/kafka/" + dataFolderName + " | grep \"kafka-log[0-9]\\+\" -o").out().trim();
    }

    /**
     * Verifies the presence or absence of KRaft metadata logs across specified volume directories within Kafka pods.
     * This method iterates through all Kafka pods retrieved based on the broker selector from the test storage configuration,
     * checking each configured volume for the presence of KRaft metadata files. The method asserts that the metadata log
     * exists only in the specified KRaft metadata volume and not in others, ensuring correct metadata log placement
     * according to the test configuration.
     *
     * @param testStorage               An instance of TestStorage containing configuration and context for the current test,
     *                                  including namespace and broker selector for identifying relevant Kafka pods.
     * @param kraftMetadataVolumeId     The volume ID expected to contain the KRaft metadata log. This method will
     *                                  assert the presence of metadata logs in this volume and their absence in others.
     * @param numberOfVolumes           The total number of volumes configured in the JBOD (Just a Bunch Of Disks) for each Kafka broker.
     *                                  This dictates how many volume directories the method will check within each Kafka pod.
     */
    public static void verifyKafkaKraftMetadataLog(final TestStorage testStorage, final int kraftMetadataVolumeId, final int numberOfVolumes) {
        final List<Pod> kafkaPods = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        int kafkaIndex = 0; // Ensure this index is managed appropriately if used outside this method context.

        for (final Pod kafkaPod : kafkaPods) {
            // Directly iterate over volumes instead of creating a list
            for (int volumeId = 0; volumeId < numberOfVolumes; volumeId++) {
                final String dir = buildDirectoryPath(volumeId, kafkaIndex);
                final int result = cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(false,
                    kafkaPod.getMetadata().getName(),
                    "kafka",
                    "/bin/bash", "-c", "test -d " + dir).returnCode();

                LOGGER.info("Kafka pod: {} the directory: {} - {}", kafkaPod.getMetadata().getName(), dir, result);

                // Assert the condition that metadata should only exist in the specified KRaft metadata volume.
                if (volumeId == kraftMetadataVolumeId) {
                    assertThat("Metadata should exist in KRaft metadata volume", result, CoreMatchers.is(0));
                } else {
                    assertThat("Metadata should not exist in non-KRaft metadata volume", result, CoreMatchers.is(1));
                }
            }
            kafkaIndex++;
        }
    }

    /**
     * Constructs the directory path for a given volume and Kafka index.
     *
     * @param volumeId      The volume ID for which the directory path is constructed.
     * @param kafkaIndex    The index of the Kafka broker.
     * @return              The constructed directory path as a String.
     */
    private static String buildDirectoryPath(int volumeId, int kafkaIndex) {
        return String.format("/var/lib/kafka/data-%d/kafka-log%d/__cluster_metadata-0", volumeId, kafkaIndex);
    }

    /**
     * Waits until the Kafka cluster in the given namespace the specified AutoRebalance state is set.
     *
     * @param namespaceName         The name of the Kubernetes namespace where the Kafka cluster resides.
     * @param clusterName           The name of the Kafka cluster.
     * @param expectedState         The expected {@link KafkaAutoRebalanceState} that is set.
     */
    public static void waitUntilKafkaHasAutoRebalanceState(final String namespaceName,
                                                           final String clusterName,
                                                           final KafkaAutoRebalanceState expectedState) {
        TestUtils.waitFor(
            String.format("Kafka cluster %s/%s and KafkaRebalance state: '%s'", clusterName, namespaceName, expectedState),
            TestConstants.GLOBAL_POLL_INTERVAL,
            TestConstants.GLOBAL_TIMEOUT,
            () -> {
                Kafka kafka = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
                if (kafka == null || kafka.getStatus() == null) {
                    return false; // Kafka cluster or its status is not available
                }

                final KafkaAutoRebalanceState currentState = kafka.getStatus().getAutoRebalance() != null
                    ? kafka.getStatus().getAutoRebalance().getState() : null;

                return currentState != null && currentState.equals(expectedState);
            }
        );
    }

    /**
     * Waits until the Kafka cluster in the given namespace reaches the specified AutoRebalance mode and brokers.
     *
     * @param namespaceName                     The name of the Kubernetes namespace where the Kafka cluster resides.
     * @param clusterName                       The name of the Kafka cluster.
     * @param expectedKafkaAutoRebalanceMode    The expected {@link KafkaAutoRebalanceMode} that the Kafka cluster should reach.
     * @param scalingBrokers                    A list of expected broker IDs that the Kafka cluster should have in AutoRebalance state.
     */
    public static void waitUntilKafkaHasExpectedAutoRebalanceModeAndBrokers(final String namespaceName,
                                                                            final String clusterName,
                                                                            final KafkaAutoRebalanceMode expectedKafkaAutoRebalanceMode,
                                                                            final List<Integer> scalingBrokers) {
        TestUtils.waitFor(
            String.format("Kafka cluster %s/%s and KafkaRebalance mode '%s' with brokers %s",
                namespaceName, clusterName, expectedKafkaAutoRebalanceMode, scalingBrokers),
            TestConstants.GLOBAL_POLL_INTERVAL,
            TestConstants.GLOBAL_TIMEOUT,
            () -> {
                final Kafka kafka = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
                if (kafka == null || kafka.getStatus() == null || kafka.getStatus().getAutoRebalance() == null) {
                    return false; // Kafka cluster, its status, or AutoRebalance status is not available
                }

                final List<KafkaAutoRebalanceStatusBrokers> autoRebalanceModeBrokersList = kafka.getStatus().getAutoRebalance().getModes();
                if (autoRebalanceModeBrokersList == null) {
                    return false;
                }

                return autoRebalanceModeBrokersList.stream()
                    .anyMatch(modeBrokers -> modeBrokers.getMode().equals(expectedKafkaAutoRebalanceMode)
                        && modeBrokers.getBrokers().equals(scalingBrokers));
            }
        );
    }

    /**
     * Executes a command in the specified broker pod for a given data directory path.
     *
     * @param namespaceName namespace name
     * @param brokerPodName the name of the broker pod.
     * @param volumeId      the volume ID for the data directory.
     * @param brokerId      the broker ID for the data directory.
     * @param command       the command to execute in the pod.
     * @return the ExecResult of the command execution.
     * @throws RuntimeException if the command execution fails.
     */
    public static ExecResult executeInBrokerPod(final String namespaceName,
                                                final String brokerPodName,
                                                final int volumeId,
                                                final int brokerId,
                                                final String command) {
        LOGGER.info("Executing command in broker {} volume {}: {}", brokerId, volumeId, command);

        final ExecResult execResult = cmdKubeClient(namespaceName)
            .execInPodContainer(brokerPodName, "kafka", "bash", "-c", command);

        if (execResult.returnCode() != 0) {
            throw new RuntimeException("Failed to execute command " + command + " in pod " + brokerPodName + ": " + execResult.out() + " " + execResult.err());
        }

        return execResult;
    }

    /**
     * Constructs the data directory path for a given broker and volume.
     *
     * @param volumeId the volume ID.
     * @param brokerId the broker ID.
     * @return the data directory path as a string.
     */
    public static String getDataDirectoryPath(final int volumeId,
                                               final int brokerId) {
        return String.format("/var/lib/kafka/data-%d/kafka-log%d", volumeId, brokerId);
    }
}

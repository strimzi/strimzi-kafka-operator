/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    private KafkaUtils() {}

    public static void waitForKafkaReady(String clusterName) {
        waitForKafkaStatus(clusterName, Ready);
    }

    public static void waitForKafkaNotReady(String clusterName) {
        waitForKafkaStatus(clusterName, NotReady);
    }

    public static void waitForKafkaStatus(String clusterName, Enum<?>  state) {
        Kafka kafka = KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
        ResourceManager.waitForResourceStatus(KafkaResource.kafkaClient(), kafka, state);
    }

    /**
     * Waits for the Kafka Status to be updated after changed. It checks the generation and observed generation to
     * ensure the status is up to date.
     *
     * @param clusterName   Name of the Kafka cluster which should be checked
     */
    public static void waitForKafkaStatusUpdate(String clusterName)   {
        LOGGER.info("Waiting for Kafka status to be updated");
        TestUtils.waitFor("KafkaStatus update", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Kafka k = KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
            return k.getMetadata().getGeneration() == k.getStatus().getObservedGeneration();
        });
    }


    public static void waitUntilKafkaStatusConditionContainsMessage(String clusterName, String namespace, String message, long timeout) {
        TestUtils.waitFor("Kafka Status with message [" + message + "]",
            Constants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                List<Condition> conditions = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getConditions();
                for (Condition condition : conditions) {
                    if (condition.getMessage().matches(message)) {
                        return true;
                    }
                }
                return false;
            });
    }

    public static void waitUntilKafkaStatusConditionContainsMessage(String clusterName, String namespace, String message) {
        waitUntilKafkaStatusConditionContainsMessage(clusterName, namespace, message, Constants.GLOBAL_STATUS_TIMEOUT);
    }

    public static void waitForZkMntr(String clusterName, Pattern pattern, int... podIndexes) {
        long timeoutMs = 120_000L;
        long pollMs = 1_000L;

        for (int podIndex : podIndexes) {
            String zookeeperPod = KafkaResources.zookeeperPodName(clusterName, podIndex);
            String zookeeperPort = String.valueOf(12181);
            waitFor("mntr", pollMs, timeoutMs, () -> {
                    try {
                        String output = cmdKubeClient().execInPod(zookeeperPod,
                            "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort).out();

                        if (pattern.matcher(output).find()) {
                            return true;
                        }
                    } catch (KubeClusterException e) {
                        LOGGER.trace("Exception while waiting for ZK to become leader/follower, ignoring", e);
                    }
                    return false;
                },
                () -> LOGGER.info("zookeeper `mntr` output at the point of timeout does not match {}:{}{}",
                    pattern.pattern(),
                    System.lineSeparator(),
                    indent(cmdKubeClient().execInPod(zookeeperPod, "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort).out()))
            );
        }
    }

    public static String getKafkaStatusCertificates(String listenerType, String namespace, String clusterName) {
        String certs = "";
        List<ListenerStatus> kafkaListeners = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getListeners();

        for (ListenerStatus listener : kafkaListeners) {
            if (listener.getType().equals(listenerType))
                certs = listener.getCertificates().toString();
        }
        certs = certs.substring(1, certs.length() - 1);
        return certs;
    }

    public static String getKafkaSecretCertificates(String secretName, String certType) {
        String secretCerts = "";
        secretCerts = kubeClient().getSecret(secretName).getData().get(certType);
        byte[] decodedBytes = Base64.getDecoder().decode(secretCerts);
        secretCerts = new String(decodedBytes, Charset.defaultCharset());

        return secretCerts;
    }

    @SuppressWarnings("unchecked")
    public static void waitForClusterStability(String clusterName) {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        Map<String, String>[] kafkaPods = new Map[1];
        Map<String, String>[] eoPods = new Map[1];
        int[] count = {0};
        zkPods[0] = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(clusterName));
        kafkaPods[0] = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));
        eoPods[0] = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Map<String, String> zkSnapshot = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(clusterName));
            Map<String, String> kafkaSnaptop = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));
            Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));
            boolean zkSameAsLast = zkSnapshot.equals(zkPods[0]);
            boolean kafkaSameAsLast = kafkaSnaptop.equals(kafkaPods[0]);
            boolean eoSameAsLast = eoSnapshot.equals(eoPods[0]);
            if (!zkSameAsLast) {
                LOGGER.info("ZK Cluster not stable");
            }
            if (!kafkaSameAsLast) {
                LOGGER.info("Kafka Cluster not stable");
            }
            if (!eoSameAsLast) {
                LOGGER.info("EO not stable");
            }
            if (zkSameAsLast
                    && kafkaSameAsLast
                    && eoSameAsLast) {
                int c = count[0]++;
                LOGGER.info("All stable for {} polls", c);
                return c > 60;
            }
            zkPods[0] = zkSnapshot;
            kafkaPods[0] = kafkaSnaptop;
            count[0] = 0;
            return false;
        });
    }

    /**
     * Method which, update/replace Kafka configuration
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static void updateSpecificConfiguration(String clusterName, String brokerConfigName, Object value) {
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
            config.put(brokerConfigName, value);
            kafka.getSpec().getKafka().setConfig(config);
            LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
        });
    }

    /**
     * Method which, extends the @see updateConfiguration(String clusterName, KafkaConfiguration kafkaConfiguration, Object value) method
     * with stability and ensures after update of Kafka resource there will be not rolling update
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static void  updateConfigurationWithStabilityWait(String clusterName, String brokerConfigName, Object value) {
        updateSpecificConfiguration(clusterName, brokerConfigName, value);
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(clusterName));
    }

    /**
     * Verifies that updated configuration was successfully changed inside Kafka CR
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static boolean verifyCrDynamicConfiguration(String clusterName, String brokerConfigName, Object value) {
        LOGGER.info("Dynamic Configuration in Kafka CR is {}={} and excepted is {}={}",
            brokerConfigName,
            KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getSpec().getKafka().getConfig().get(brokerConfigName),
            brokerConfigName,
            value);

        return KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getSpec().getKafka().getConfig().get(brokerConfigName).equals(value);
    }

    /**
     * Method, which, verifying that updating configuration were successfully changed inside Kafka pods
     * @param kafkaPodNamePrefix prefix of Kafka pods
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     * @return
     * true = if specific property match the excepted property
     * false = if specific property doesn't match the excepted property
     */
    public static boolean verifyPodDynamicConfiguration(String kafkaPodNamePrefix, String brokerConfigName, Object value) {

        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(kafkaPodNamePrefix);

        for (Pod pod : kafkaPods) {

            TestUtils.waitFor("Wait until dyn.configuration is changed", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
                () -> {
                    String result = cmdKubeClient().execInPod(pod.getMetadata().getName(), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();

                    LOGGER.debug("This dyn.configuration {} inside the Kafka pod {}", result, pod.getMetadata().getName());

                    if (!result.contains(brokerConfigName + "=" + value)) {
                        LOGGER.error("Kafka Pod {} doesn't contain {} with value {}", pod.getMetadata().getName(), brokerConfigName, value);
                        LOGGER.error("Kafka configuration {}", result);
                        return false;
                    }
                    return true;
                });
        }
        return true;
    }

    /**
     * Method, which load all supported kafka configuration generated by #KafkaConfigModelGenerator in config-model-generator
     * @param kafkaVersion specific kafka version
     * @return JsonObject all supported kafka properties
     */
    @SuppressFBWarnings("RR_NOT_CHECKED")
    public static JsonObject loadSupportedKafkaConfigs(String kafkaVersion) {

        File file = new File("../cluster-operator/src/main/resources/kafka-" + kafkaVersion + "-config-model.json");
        byte[] data = new byte[0];

        try (FileInputStream fis = new FileInputStream(file)) {

            data = new byte[(int) file.length()];
            fis.read(data);

        } catch (IOException e) {
            e.printStackTrace();
        }

        String kafkaConfigs = new String(data, Charset.defaultCharset());

        return new JsonObject(kafkaConfigs);
    }

    /**
     * Method, which process all supported configs by Kafka and filter all which are not dynamic
     * @param kafkaVersion specific kafka version
     * @return Map<String, Object> all dynamic properties for specific kafka version
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:BooleanExpressionComplexity", "unchecked"})
    public static Map<String, Object> getDynamicConfigurationProperties(String kafkaVersion)  {

        JsonObject kafkaConfig = KafkaUtils.loadSupportedKafkaConfigs(kafkaVersion);

        Map<String, Object> dynamicConfigs = kafkaConfig.getJsonObject("configs")
            .getMap()
            .entrySet()
            .stream()
            .filter(a ->
                // ignoring everything which is READ_ONLY
                !((LinkedHashMap<String, String>) a.getValue()).get("scope").equals("READ_ONLY") &&
                    // filtering configs with following prefixes
                    // "listeners, advertised., broker., listener., host.name, port, inter.broker.listener.name, sasl., ssl.,
                    // security., password., principal.builder.class, log.dir, zookeeper.connect, zookeeper.set.acl, authorizer.,
                    // super.user, cruise.control.metrics.topic, cruise.control.metrics.reporter.bootstrap.servers
                    !(
                        a.getKey().startsWith("listeners") ||
                            a.getKey().startsWith("advertised") ||
                            a.getKey().startsWith("broker") ||
                            a.getKey().startsWith("listener") ||
                            a.getKey().startsWith("host.name") ||
                            a.getKey().startsWith("port") ||
                            a.getKey().startsWith("inter.broker.listener.name") ||
                            a.getKey().startsWith("sasl") ||
                            a.getKey().startsWith("ssl") ||
                            a.getKey().startsWith("security") ||
                            a.getKey().startsWith("password") ||
                            a.getKey().startsWith("principal.builder.class") ||
                            a.getKey().startsWith("log.dir") ||
                            a.getKey().startsWith("zookeeper.connect") ||
                            a.getKey().startsWith("zookeeper.set.acl") ||
                            a.getKey().startsWith("authorizer") ||
                            a.getKey().startsWith("super.user") ||
                            a.getKey().startsWith("cruise.control.metrics.topic") ||
                            a.getKey().startsWith("cruise.control.metrics.reporter.bootstrap.servers"))
            )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return dynamicConfigs;
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.KafkaDynamicConfiguration;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
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
     * @param kafkaDynamicConfiguration enum instance, which defines all supported configurations
     * @param value value of specific property
     */
    public static void updateSpecificConfiguration(String clusterName, KafkaDynamicConfiguration kafkaDynamicConfiguration, Object value) {
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
            config.put(kafkaDynamicConfiguration.toString(), value);
            kafka.getSpec().getKafka().setConfig(config);
            LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
        });
    }

    /**
     * Method which, extends the @see updateConfiguration(String clusterName, KafkaConfiguration kafkaConfiguration, Object value) method
     * with stability and ensures after update of Kafka resource there will be not rolling update
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param kafkaDynamicConfiguration enum instance, which defines all supported configurations
     * @param value value of specific property
     */
    public static void updateConfigurationWithStabilityWait(String clusterName, KafkaDynamicConfiguration kafkaDynamicConfiguration, Object value) {
        updateSpecificConfiguration(clusterName, kafkaDynamicConfiguration, value);
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(clusterName));
    }

    /**
     * Method, which encapsulates the update phase of dyn. configuration + verifying that updating configuration were successfully done
     * @param kafkaDynamicConfiguration enum instance, which defines all supported configurations
     * @param value value of specific property
     */
    public static void verifyDynamicConfiguration(String clusterName, KafkaDynamicConfiguration kafkaDynamicConfiguration, Object value) {
        KafkaUtils.updateConfigurationWithStabilityWait(clusterName, kafkaDynamicConfiguration, value);

        boolean result = KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getSpec().getKafka().getConfig().get(kafkaDynamicConfiguration.toString()) == value;

        if (!result) {
            throw new AssertionError(KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getSpec().getKafka().getConfig().get(kafkaDynamicConfiguration.toString() + " value doesn't match to expected value " + value));
        }
    }
}

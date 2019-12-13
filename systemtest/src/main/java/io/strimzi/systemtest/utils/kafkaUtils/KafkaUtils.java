/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    private KafkaUtils() {}

    public static void waitUntilKafkaCRIsReady(String clusterName) {
        LOGGER.info("Waiting till Kafka CR will be ready");
        TestUtils.waitFor("Waiting for Kafka resource status is ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->   Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0).getType().equals("Ready") &&
                Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0).getStatus().equals("True")
        );
        LOGGER.info("Kafka CR will be ready");
    }

    public static void waitUntilKafkaStatusConditionIsPresent(String clusterName) {
        LOGGER.info("Waiting till kafka resource status is present");
        TestUtils.waitFor("Waiting for Kafka resource status is ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->  Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0) != null
        );
        LOGGER.info("Kafka resource status is present");
    }

    public static void waitForZkMntr(String clusterName, Pattern pattern, int... podIndexes) {
        long timeoutMs = 120_000L;
        long pollMs = 1_000L;

        for (int podIndex : podIndexes) {
            String zookeeperPod = KafkaResources.zookeeperPodName(clusterName, podIndex);
            String zookeeperPort = String.valueOf(2181 * 10 + podIndex);
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

}

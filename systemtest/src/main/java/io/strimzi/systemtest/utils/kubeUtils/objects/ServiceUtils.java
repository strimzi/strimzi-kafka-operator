/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ServiceUtils {

    private static final Logger LOGGER = LogManager.getLogger(ServiceUtils.class);

    private ServiceUtils() { }

    public static void waitForKafkaServiceLabelsChange(String serviceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Kafka service label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for Kafka service label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                        kubeClient().getService(serviceName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForKafkaServiceLabelsDeletion(String serviceName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Kafka service label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for Kafka service label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                    kubeClient().getService(serviceName).getMetadata().getLabels().get(labelKey) == null
            );
        }
    }

    public static void waitForLoadBalancerService(String serviceName) {
        LOGGER.info("Waiting when Service {} in namespace {} is ready", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("LoadBalancer service " + serviceName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getClient().services().inNamespace(kubeClient().getNamespace()).withName(serviceName).get().getSpec().getExternalIPs().size() > 0);
    }

    public static void waitForNodePortService(String serviceName) throws InterruptedException {
        LOGGER.info("Waiting when Service {} in namespace {} is ready", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("NodePort service " + serviceName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getClient().services().inNamespace(kubeClient().getNamespace()).withName(serviceName).get().getSpec().getPorts().get(0).getNodePort() != null);

        Thread.sleep(10000);
    }

    /**
     * Wait until Service of the given name will be deleted.
     * @param serviceName service name
     */
    public static void waitForServiceDeletion(String serviceName) {
        LOGGER.info("Waiting when Service {} in namespace {} has been deleted", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("Service " + serviceName + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getService(serviceName) == null);
    }

    /**
     * Wait until Service of the given name will be recovered.
     * @param serviceName service name
     * @param serviceUid service original uid
     */
    public static void waitForServiceRecovery(String serviceName, String serviceUid) {
        LOGGER.info("Waiting when Service {}-{} in namespace {} is recovered", serviceName, serviceUid, kubeClient().getNamespace());

        TestUtils.waitFor("Service " + serviceName + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getServiceUid(serviceName).equals(serviceUid));
    }

    public static void waitUntilAddressIsReachable(String address) {
        LOGGER.info("Waiting till address {} is reachable", address);
        TestUtils.waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    InetAddress.getByName(kubeClient().getService("my-cluster-kafka-external-bootstrap").getStatus().getLoadBalancer().getIngress().get(0).getHostname());
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });
        LOGGER.info("Address {} is reachable", address);
    }
}

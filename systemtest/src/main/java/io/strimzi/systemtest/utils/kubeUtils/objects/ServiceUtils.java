/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ServiceUtils {

    private static final Logger LOGGER = LogManager.getLogger(ServiceUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.SERVICE);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private ServiceUtils() { }

    public static void waitForServiceLabelsChange(String serviceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Service label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Service label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.GLOBAL_TIMEOUT, () ->
                        kubeClient().getService(serviceName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForServiceLabelsDeletion(String serviceName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Service label {} change to {}", labelKey, null);
            TestUtils.waitFor("Service label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    kubeClient().getService(serviceName).getMetadata().getLabels().get(labelKey) == null
            );
        }
    }

    public static void waitForLoadBalancerService(String serviceName) {
        LOGGER.info("Waiting for Service {} in namespace {}", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("LoadBalancer service " + serviceName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient().getClient().services().inNamespace(kubeClient().getNamespace()).withName(serviceName).get().getSpec().getExternalIPs().size() > 0);
        LOGGER.info("Service {} in namespace {} is ready", serviceName, kubeClient().getNamespace());
    }

    public static void waitForNodePortService(String serviceName) throws InterruptedException {
        LOGGER.info("Waiting for Service {} in namespace {}", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("NodePort service " + serviceName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient().getClient().services().inNamespace(kubeClient().getNamespace()).withName(serviceName).get().getSpec().getPorts().get(0).getNodePort() != null);

        Thread.sleep(10000);
        LOGGER.info("Service {} in namespace {} is ready", serviceName, kubeClient().getNamespace());
    }

    /**
     * Wait until Service of the given name will be deleted.
     * @param serviceName service name
     */
    public static void waitForServiceDeletion(String serviceName) {
        LOGGER.info("Waiting for Service {} deletion in namespace {}", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("Service " + serviceName + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> kubeClient().getService(serviceName) == null);
        LOGGER.info("Service {} in namespace {} was deleted", serviceName, kubeClient().getNamespace());
    }

    /**
     * Wait until Service of the given name will be recovered.
     * @param serviceName service name
     * @param serviceUid service original uid
     */
    public static void waitForServiceRecovery(String serviceName, String serviceUid) {
        LOGGER.info("Waiting when Service {}-{} in namespace {} will be recovered", serviceName, serviceUid, kubeClient().getNamespace());

        TestUtils.waitFor("Service " + serviceName + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !kubeClient().getServiceUid(serviceName).equals(serviceUid));
        LOGGER.info("{} in namespace {} is recovered", serviceName, kubeClient().getNamespace());
    }

    public static void waitUntilAddressIsReachable(String address) {
        LOGGER.info("Wait until address {} is reachable", address);
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

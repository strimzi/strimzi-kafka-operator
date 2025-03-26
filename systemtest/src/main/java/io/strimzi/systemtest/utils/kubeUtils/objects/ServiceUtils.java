/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

public class ServiceUtils {

    private static final Logger LOGGER = LogManager.getLogger(ServiceUtils.class);

    private ServiceUtils() { }

    public static void waitForServiceLabelsChange(String namespaceName, String serviceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Service label to change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Service label to change " + entry.getKey() + " -> " + entry.getValue(), TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    TestConstants.GLOBAL_TIMEOUT, () ->
                        KubeResourceManager.get().kubeClient().getClient().services().inNamespace(namespaceName).withName(serviceName).get().getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    /**
     * Wait until Service of the given name will be recovered.
     * @param serviceName service name
     * @param serviceUid service original uid
     */
    public static void waitForServiceRecovery(String namespaceName, String serviceName, String serviceUid) {
        LOGGER.info("Waiting for Service: {}/{}-{} to be recovered", namespaceName, serviceName, serviceUid);

        TestUtils.waitFor("recovery of Service: " + serviceName + "/" + namespaceName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !KubeResourceManager.get().kubeClient().getClient().services().inNamespace(namespaceName).withName(serviceName).get().getMetadata().getUid().equals(serviceUid));
        LOGGER.info("Service: {}/{} is recovered", namespaceName, serviceName);
    }

    public static void waitUntilAddressIsReachable(String address) {
        LOGGER.info("Waiting for address: {} to be reachable", address);
        TestUtils.waitFor("", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    InetAddress.getByName(address);
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });
        LOGGER.info("Address: {} is reachable", address);
    }
}

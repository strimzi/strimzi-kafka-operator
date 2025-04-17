/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.strimzi.systemtest.TestConstants;

import java.util.Objects;

import static io.strimzi.operator.common.Util.hashStub;

public class KafkaResourceNames {
    /**
     * Returns the name of the controller's StrimziPodSet
     *
     * @param clusterName name of the Kafka cluster
     * @return component name of controller
     */
    public static String getControllerComponentName(String clusterName) {
        return getStrimziPodSetName(clusterName, getControllerPoolName(clusterName));
    }

    /**
     * Returns the name of the broker's StrimziPodSet
     *
     * @param clusterName name of the Kafka cluster
     * @return component name of broker
     */
    public static String getBrokerComponentName(String clusterName) {
        return getStrimziPodSetName(clusterName, getBrokerPoolName(clusterName));
    }

    public static String getStrimziPodSetName(String clusterName, String nodePoolName) {
        return String.join("-", clusterName, Objects.requireNonNullElseGet(nodePoolName, () -> getBrokerPoolName(clusterName)));
    }

    public static int getPodNumFromPodName(String componentName, String podName) {
        return Integer.parseInt(podName.replace(componentName + "-", ""));
    }

    public static String getBrokerPoolName(String clusterName) {
        return TestConstants.BROKER_ROLE_PREFIX + hashStub(clusterName);
    }

    public static String getControllerPoolName(String clusterName) {
        return TestConstants.CONTROLLER_ROLE_PREFIX + hashStub(clusterName);
    }

    public static String getMixedPoolName(String clusterName) {
        return TestConstants.MIXED_ROLE_PREFIX + hashStub(clusterName);
    }
}

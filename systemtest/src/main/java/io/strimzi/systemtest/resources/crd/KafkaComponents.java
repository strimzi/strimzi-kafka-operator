/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.strimzi.systemtest.TestConstants;

import java.util.Objects;

import static io.strimzi.operator.common.Util.hashStub;

/**
 * Class containing simple methods returning various names for Kafka resources.
 */
public class KafkaComponents {
    /**
     * Private constructor to prevent instantiation.
     */
    private KafkaComponents() {
        // private constructor
    }

    /**
     * Returns the name of the controller's StrimziPodSet.
     *
     * @param clusterName name of the Kafka cluster.
     * @return component name of controller.
     */
    public static String getControllerPodSetName(String clusterName) {
        return getPodSetName(clusterName, getControllerPoolName(clusterName));
    }

    /**
     * Returns the name of the broker's StrimziPodSet.
     *
     * @param clusterName name of the Kafka cluster.
     * @return component name of broker.
     */
    public static String getBrokerPodSetName(String clusterName) {
        return getPodSetName(clusterName, getBrokerPoolName(clusterName));
    }

    /**
     * Returns the name of the StrimziPodSet.
     *
     * @param clusterName   name of the Kafka cluster.
     * @param nodePoolName  name of the StrimziPodSet.
     *
     * @return  name of the StrimziPodSet.
     */
    public static String getPodSetName(String clusterName, String nodePoolName) {
        return String.join("-", clusterName, Objects.requireNonNullElseGet(nodePoolName, () -> getBrokerPoolName(clusterName)));
    }

    /**
     * Returns the Pod number from its name.
     *
     * @param componentName   name of the component (from which the full name is made of).
     * @param podName         name of the Pod.
     *
     * @return  Pod number from its name.
     */
    public static int getPodNumFromPodName(String componentName, String podName) {
        return Integer.parseInt(podName.replace(componentName + "-", ""));
    }

    /**
     * Returns broker NodePool name from {@param clusterName}.
     *
     * @param clusterName   name of the Kafka cluster.
     *
     * @return  broker NodePool name.
     */
    public static String getBrokerPoolName(String clusterName) {
        return TestConstants.BROKER_ROLE_PREFIX + hashStub(clusterName);
    }

    /**
     * Returns controller NodePool name from {@param clusterName}.
     *
     * @param clusterName   name of the Kafka cluster.
     *
     * @return  controller NodePool name.
     */
    public static String getControllerPoolName(String clusterName) {
        return TestConstants.CONTROLLER_ROLE_PREFIX + hashStub(clusterName);
    }

    /**
     * Returns mixed-roles NodePool name from {@param clusterName}.
     *
     * @param clusterName   name of the Kafka cluster.
     *
     * @return  mixed-roles NodePool name.
     */
    public static String getMixedPoolName(String clusterName) {
        return TestConstants.MIXED_ROLE_PREFIX + hashStub(clusterName);
    }
}

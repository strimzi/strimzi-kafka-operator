/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.Map;

/**
 * Utility methods for working with resource request and limit values from the resource requirements section
 * of Strimzi custom resources. Used for evaluating whether Cruise Control capacity settings for CPU are properly
 * configured.
 */
public class ResourceRequirementsUtils {

    /**
     * Enum representing resource types used in the resource requirements section of Strimzi custom resources.
     */
    public enum ResourceType {
        /**
         * Represents the memory resource type (e.g., "memory").
         */
        MEMORY("memory"),

        /**
         * Represents the CPU resource type (e.g., "cpu").
         */
        CPU("cpu");

        private final String value;

        ResourceType(String value) {
            this.value = value;
        }

        /**
         * Returns the string value of the resource type.
         *
         * @return the resource type name.
         */
        public String value() {
            return value;
        }
    }

    /**
     * Enum representing resource requirement types used in the resource requirements section Strimzi custom resources.
     */
    private enum ResourceRequirementsType {
        /**
         * Represents the resource request value.
         */
        REQUEST,

        /**
         * Represents the resource limit value.
         */
        LIMIT;
    }

    private static Quantity getQuantity(ResourceRequirements resources,
                                        ResourceRequirementsType requirementType,
                                        ResourceRequirementsUtils.ResourceType resourceType) {
        if (resources == null || resourceType == null) {
            return null;
        }

        Map<String, Quantity> resourceRequirement = switch (requirementType) {
            case REQUEST -> resources.getRequests();
            case LIMIT -> resources.getLimits();
        };

        return resourceRequirement != null ? resourceRequirement.get(resourceType.value()) : null;
    }

    /**
     * Retrieves the CPU resource request quantity from the given resource requirements.
     *
     * @param resources the resource requirements containing CPU and memory requests and limits
     * @return the requested CPU quantity, or {@code null} if not specified
     */
    private static Quantity getCpuRequest(ResourceRequirements resources) {
        return getQuantity(resources, ResourceRequirementsType.REQUEST, ResourceType.CPU);
    }

    /**
     * Retrieves the CPU resource limit quantity from the given resource requirements.
     *
     * @param resources the resource requirements containing CPU and memory requests and limits
     * @return the CPU limit quantity, or {@code null} if not specified
     */
    private static Quantity getCpuLimit(ResourceRequirements resources) {
        return getQuantity(resources, ResourceRequirementsType.LIMIT, ResourceType.CPU);
    }

    /**
     * Checks whether all Kafka broker pods have their CPU resource requests equal to their CPU limits.
     *
     * @param kafkaBrokerResources a map of broker pod names to their {@link ResourceRequirements}
     * @return {@code true} if all brokers have matching CPU requests and limits; {@code false} otherwise
     */
    protected static boolean cpuRequestsMatchLimits(Map<String, ResourceRequirements> kafkaBrokerResources) {
        if (kafkaBrokerResources == null) {
            return false;
        }
        for (ResourceRequirements resourceRequirements : kafkaBrokerResources.values()) {
            Quantity request = getCpuRequest(resourceRequirements);
            Quantity limit = getCpuLimit(resourceRequirements);
            if (request == null || limit == null || request.compareTo(limit) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Derives the CPU capacity from the resource requirements section of Strimzi custom resource.
     *
     * @param resourceRequirements The Strimzi custom resource requirements containing CPU requests and/or limits.
     * @return The CPU capacity as a {@link String}, or {@code null} if no CPU values are defined.
     */
    protected static String getCpuBasedOnRequirements(ResourceRequirements resourceRequirements) {
        Quantity request = getCpuRequest(resourceRequirements);
        Quantity limit = getCpuLimit(resourceRequirements);

        if (request != null) {
            return request.toString();
        } else if (limit != null) {
            return limit.toString();
        } else {
            return null;
        }
    }
}
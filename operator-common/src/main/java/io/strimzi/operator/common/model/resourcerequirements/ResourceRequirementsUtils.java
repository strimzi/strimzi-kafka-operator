/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.resourcerequirements;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.Map;

import static io.strimzi.operator.common.model.resourcerequirements.ResourceRequirementsType.LIMIT;
import static io.strimzi.operator.common.model.resourcerequirements.ResourceRequirementsType.REQUEST;
import static io.strimzi.operator.common.model.resourcerequirements.ResourceType.CPU;
import static io.strimzi.operator.common.model.resourcerequirements.ResourceType.MEMORY;

/**
 * Utility methods for working with the resources section of custom resources.
 */
public class ResourceRequirementsUtils {

    private static Quantity getQuantity(ResourceRequirements resources,
                                        ResourceRequirementsType requirementType,
                                        ResourceType resourceType) {
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
    public static Quantity getCpuRequest(ResourceRequirements resources) {
        return getQuantity(resources, REQUEST, CPU);
    }

    /**
     * Retrieves the CPU resource limit quantity from the given resource requirements.
     *
     * @param resources the resource requirements containing CPU and memory requests and limits
     * @return the CPU limit quantity, or {@code null} if not specified
     */
    public static Quantity getCpuLimit(ResourceRequirements resources) {
        return getQuantity(resources, LIMIT, CPU);
    }

    /**
     * Retrieves the memory resource request quantity from the given resource requirements.
     *
     * @param resources the resource requirements containing CPU and memory requests and limits
     * @return the requested memory quantity, or {@code null} if not specified
     */
    public static Quantity getMemoryRequest(ResourceRequirements resources) {
        return getQuantity(resources, REQUEST, MEMORY);
    }

    /**
     * Retrieves the memory resource limit quantity from the given resource requirements.
     *
     * @param resources the resource requirements containing CPU and memory requests and limits
     * @return the memory limit quantity, or {@code null} if not specified
     */
    public static Quantity getMemoryLimit(ResourceRequirements resources) {
        return getQuantity(resources, LIMIT, MEMORY);
    }
}

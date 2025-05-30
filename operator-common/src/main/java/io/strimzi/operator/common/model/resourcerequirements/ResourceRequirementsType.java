/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.resourcerequirements;

/**
 * Enum representing the types of resource requirements used in Kubernetes container specifications.
 *
 * <p>In Kubernetes, containers can specify both <b>requests</b> and <b>limits</b> for compute resources
 * such as CPU and memory. These values influence scheduling and runtime resource enforcement:</p>
 *
 * <ul>
 *   <li>{@link #REQUEST} - The minimum amount of a resource that the container is guaranteed.</li>
 *   <li>{@link #LIMIT} - The maximum amount of a resource that the container is allowed to use.</li>
 * </ul>
 *
 * @see <a href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/">Kubernetes: Managing Resources for Containers</a>
 */
public enum ResourceRequirementsType {
    /**
     * Represents the resource request value.
     */
    REQUEST,

    /**
     * Represents the resource limit value.
     */
    LIMIT;
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.List;
import java.util.Set;

/**
 * Interface that must be implemented by components that support sidecar containers.
 * This interface defines the contract for component-specific sidecar behavior.
 * 
 * This interface is purely functional - all methods take the required data as parameters
 * rather than depending on component state via getter methods.
 */
public interface SidecarInterface {
    /**
     * Validates sidecar containers for the given pod template.
     * This method is mandatory and must be implemented by all components supporting sidecars.
     * 
     * Typical implementation pattern:
     * <pre>
     * public void validateSidecarContainers(Reconciliation reconciliation, PodTemplate templatePod, Set&lt;Integer&gt; componentPorts, String componentType) {
     *     SidecarUtils.validateSidecarContainersWithTemplate(
     *         reconciliation,
     *         templatePod,
     *         componentPorts,
     *         componentType
     *     );
     * }
     * </pre>
     *
     * @param reconciliation Reconciliation context for logging and error reporting
     * @param templatePod    Pod template containing sidecar container definitions
     * @param componentPorts Set of ports used by the component that sidecars cannot use
     * @param componentType  Component type for error reporting (e.g., "kafka", "connect", "bridge")
     * @throws InvalidResourceException if sidecar validation fails
     */
    void validateSidecarContainers(Reconciliation reconciliation, PodTemplate templatePod, Set<Integer> componentPorts, String componentType);

    /**
     * Creates sidecar containers for the given pod template.
     * This method is mandatory and must be implemented by all components supporting sidecars.
     * 
     * Typical implementation pattern:
     * <pre>
     * public List&lt;Container&gt; createSidecarContainers(PodTemplate templatePod, ImagePullPolicy imagePullPolicy) {
     *     return SidecarUtils.convertSidecarContainers(templatePod, imagePullPolicy);
     * }
     * </pre>
     *
     * @param templatePod     Pod template containing sidecar container definitions
     * @param imagePullPolicy Image pull policy to apply to sidecar containers
     * @return List of sidecar containers
     */
    List<Container> createSidecarContainers(PodTemplate templatePod, ImagePullPolicy imagePullPolicy);
}
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.SidecarContainer;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for validating sidecar container configurations
 */
public class SidecarUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(SidecarUtils.class.getName());

    // Reserved container names used by Strimzi
    private static final Set<String> RESERVED_CONTAINER_NAMES = Set.of(
            "kafka",
            "kafka-init"
    );

    // Reserved port numbers used by Strimzi Kafka
    private static final Set<Integer> RESERVED_PORTS = Set.of(
            9090, // CONTROLPLANE_PORT
            9091, // REPLICATION_PORT
            8443, // KAFKA_AGENT_PORT
            9404, // Default JMX Prometheus Exporter port
            9999  // Default JMX port
    );

    private SidecarUtils() {
        // Utility class
    }

    /**
     * Validates a list of sidecar containers for conflicts with Strimzi internal definitions
     *
     * @param reconciliation      Reconciliation context
     * @param sidecarContainers  List of SidecarContainer objects to validate
     * @param userDefinedPorts   Set of user-defined listener ports to avoid conflicts
     * @param componentType      Component type for path generation (e.g., "kafka", "connect", "bridge")
     * @throws InvalidResourceException if validation fails
     */
    public static void validateSidecarContainers(Reconciliation reconciliation, 
                                                List<SidecarContainer> sidecarContainers,
                                                Set<Integer> userDefinedPorts,
                                                String componentType) {
        if (sidecarContainers == null || sidecarContainers.isEmpty()) {
            return;
        }

        List<String> errors = new ArrayList<>();
        Set<String> containerNames = new HashSet<>();
        Set<Integer> usedPorts = new HashSet<>(RESERVED_PORTS);
        
        // Add user-defined listener ports to reserved ports
        if (userDefinedPorts != null) {
            usedPorts.addAll(userDefinedPorts);
        }

        for (int i = 0; i < sidecarContainers.size(); i++) {
            SidecarContainer container = sidecarContainers.get(i);
            String path = String.format(".spec.%s.template.pod.sidecarContainers[%d]", componentType, i);

            // Validate container basics
            validateContainerBasics(container, path, errors);
            
            // Validate container name uniqueness and conflicts
            validateContainerName(container, path, containerNames, errors);
            
            // Validate port conflicts
            validateContainerPorts(container, path, usedPorts, errors);
            
            // Validate volume mount conflicts (only within same container)
            validateVolumeMounts(container, path, errors);
            
            // Validate resources
            validateResources(container, path, errors);
        }

        if (!errors.isEmpty()) {
            String errorMessage = "Sidecar container validation failed: " + String.join(", ", errors);
            LOGGER.errorCr(reconciliation, errorMessage);
            throw new InvalidResourceException(errorMessage);
        }
    }

    /**
     * Validates a list of sidecar containers for conflicts with Strimzi internal definitions
     * Backward compatibility method - defaults to "kafka" component type
     *
     * @param reconciliation      Reconciliation context
     * @param sidecarContainers  List of SidecarContainer objects to validate
     * @param userDefinedPorts   Set of user-defined listener ports to avoid conflicts
     * @throws InvalidResourceException if validation fails
     */
    public static void validateSidecarContainers(Reconciliation reconciliation, 
                                                List<SidecarContainer> sidecarContainers,
                                                Set<Integer> userDefinedPorts) {
        validateSidecarContainers(reconciliation, sidecarContainers, userDefinedPorts, "kafka");
    }

    /**
     * Validates that volume mount references exist in the provided pod volumes
     *
     * @param reconciliation      Reconciliation context
     * @param sidecarContainers  List of sidecar containers to validate
     * @param podVolumes         List of volumes defined in the pod template
     * @param componentType      Component type for path generation (e.g., "kafka", "connect", "bridge")
     * @throws InvalidResourceException if validation fails
     */
    public static void validateVolumeReferences(Reconciliation reconciliation, 
                                               List<SidecarContainer> sidecarContainers,
                                               List<AdditionalVolume> podVolumes,
                                               String componentType) {
        if (sidecarContainers == null || sidecarContainers.isEmpty()) {
            return;
        }

        Set<String> availableVolumeNames = new HashSet<>();
        if (podVolumes != null) {
            availableVolumeNames.addAll(podVolumes.stream().map(AdditionalVolume::getName).collect(Collectors.toSet()));
        }

        List<String> errors = new ArrayList<>();

        for (int i = 0; i < sidecarContainers.size(); i++) {
            SidecarContainer container = sidecarContainers.get(i);
            String path = String.format(".spec.%s.template.pod.sidecarContainers[%d]", componentType, i);

            if (container.getVolumeMounts() != null) {
                for (int j = 0; j < container.getVolumeMounts().size(); j++) {
                    VolumeMount mount = container.getVolumeMounts().get(j);
                    String mountPath = path + ".volumeMounts[" + j + "]";
                    
                    if (mount.getName() != null && !availableVolumeNames.contains(mount.getName())) {
                        errors.add(mountPath + ".name '" + mount.getName() + 
                                 "' references a volume that does not exist in template.pod.volumes");
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            String errorMessage = "Sidecar container volume reference validation failed: " + String.join(", ", errors);
            LOGGER.errorCr(reconciliation, errorMessage);
            throw new InvalidResourceException(errorMessage);
        }
    }

    /**
     * Validates that volume mount references exist in the provided pod volumes
     * Backward compatibility method - defaults to "kafka" component type
     *
     * @param reconciliation      Reconciliation context
     * @param sidecarContainers  List of sidecar containers to validate
     * @param podVolumes         List of volumes defined in the pod template
     * @throws InvalidResourceException if validation fails
     */
    public static void validateVolumeReferences(Reconciliation reconciliation, 
                                               List<SidecarContainer> sidecarContainers,
                                               List<AdditionalVolume> podVolumes) {
        validateVolumeReferences(reconciliation, sidecarContainers, podVolumes, "kafka");
    }

    private static void validateContainerBasics(SidecarContainer container, String path, List<String> errors) {
        if (container.getName() == null || container.getName().trim().isEmpty()) {
            errors.add(path + ".name is required and cannot be empty");
        }
        
        if (container.getImage() == null || container.getImage().trim().isEmpty()) {
            errors.add(path + ".image is required and cannot be empty");
        }
    }

    private static void validateContainerName(SidecarContainer container, String path, 
                                            Set<String> containerNames, List<String> errors) {
        String name = container.getName();
        if (name != null) {
            // Check for reserved names
            if (RESERVED_CONTAINER_NAMES.contains(name)) {
                errors.add(path + ".name '" + name + "' is reserved by Strimzi and cannot be used");
            }
            
            // Check for duplicate names
            if (containerNames.contains(name)) {
                errors.add(path + ".name '" + name + "' is already used by another sidecar container");
            } else {
                containerNames.add(name);
            }
            
            // Validate name format (Kubernetes container name rules)
            if (!name.matches("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")) {
                errors.add(path + ".name '" + name + "' must match regex ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$");
            }
        }
    }

    private static void validateContainerPorts(SidecarContainer container, String path, 
                                             Set<Integer> usedPorts, List<String> errors) {
        if (container.getPorts() != null) {
            for (int j = 0; j < container.getPorts().size(); j++) {
                ContainerPort port = container.getPorts().get(j);
                String portPath = path + ".ports[" + j + "]";
                
                if (port.getContainerPort() != null) {
                    int portNumber = port.getContainerPort();
                    
                    // Check for reserved ports
                    if (RESERVED_PORTS.contains(portNumber)) {
                        errors.add(portPath + ".containerPort " + portNumber + " is reserved by Strimzi");
                    }
                    
                    // Check for duplicate ports within the pod
                    if (usedPorts.contains(portNumber)) {
                        errors.add(portPath + ".containerPort " + portNumber + " is already in use");
                    } else {
                        usedPorts.add(portNumber);
                    }
                    
                    // Validate port range
                    if (portNumber < 1 || portNumber > 65535) {
                        errors.add(portPath + ".containerPort must be between 1 and 65535");
                    }
                }
                
                // Validate port name if specified
                if (port.getName() != null && !port.getName().matches("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")) {
                    errors.add(portPath + ".name must match regex ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$");
                }
            }
        }
    }

    private static void validateVolumeMounts(SidecarContainer container, String path, List<String> errors) {
        if (container.getVolumeMounts() != null) {
            Set<String> mountPaths = new HashSet<>();
            
            for (int j = 0; j < container.getVolumeMounts().size(); j++) {
                VolumeMount mount = container.getVolumeMounts().get(j);
                String mountPath = path + ".volumeMounts[" + j + "]";
                
                if (mount.getMountPath() != null) {
                    String mountPathValue = mount.getMountPath();
                    
                    // Check for duplicate mount paths within the same container only
                    if (mountPaths.contains(mountPathValue)) {
                        errors.add(mountPath + ".mountPath '" + mountPathValue + 
                                 "' is already used by another volume mount in the same container");
                    } else {
                        mountPaths.add(mountPathValue);
                    }
                    
                    // Validate mount path format (must be absolute)
                    if (!mountPathValue.startsWith("/")) {
                        errors.add(mountPath + ".mountPath must be an absolute path");
                    }
                }
                
                if (mount.getName() == null || mount.getName().trim().isEmpty()) {
                    errors.add(mountPath + ".name is required");
                }
            }
        }
    }

    private static void validateResources(SidecarContainer container, String path, List<String> errors) {
        if (container.getResources() != null) {
            try {
                ModelUtils.validateComputeResources(container.getResources(), path + ".resources");
            } catch (InvalidResourceException e) {
                errors.add(e.getMessage());
            }
        }
    }
}
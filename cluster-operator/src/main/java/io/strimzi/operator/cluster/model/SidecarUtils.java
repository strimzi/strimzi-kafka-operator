/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.SidecarContainer;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
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

    // Common reserved port numbers used by Strimzi components for monitoring
    private static final Set<Integer> COMMON_RESERVED_PORTS = Set.of(
            9404, // Default JMX Prometheus Exporter port
            9999  // Default JMX port
    );

    private SidecarUtils() {
        // Utility class
    }

    /**
     * Gets the reserved container names for a specific component type.
     * Components can override this to provide their own reserved names.
     *
     * @param componentType Component type (e.g., "kafka", "connect", "bridge")
     * @return Set of reserved container names for the component
     */
    public static Set<String> getReservedContainerNames(String componentType) {
        switch (componentType) {
            case "kafka":
                return Set.of("kafka", "kafka-init");
            case "connect":
            case "kafka-connect":
                return Set.of("kafka-connect");
            case "bridge":
            case "kafka-bridge":
                return Set.of("kafka-bridge");
            default:
                return Set.of();
        }
    }

    /**
     * Gets the reserved port numbers for a specific component type.
     * Components can override this to provide their own reserved ports.
     *
     * @param componentType Component type (e.g., "kafka", "connect", "bridge")
     * @return Set of reserved port numbers for the component
     */
    public static Set<Integer> getReservedPorts(String componentType) {
        Set<Integer> ports = new HashSet<>(COMMON_RESERVED_PORTS);
        
        switch (componentType) {
            case "kafka":
                ports.addAll(Set.of(9090, 9091, 8443)); // CONTROLPLANE_PORT, REPLICATION_PORT, KAFKA_AGENT_PORT
                break;
            case "connect":
            case "kafka-connect":
                ports.add(8083); // REST_API_PORT
                break;
            case "bridge":
            case "kafka-bridge":
                ports.add(8080); // DEFAULT_REST_API_PORT
                break;
        }
        
        return ports;
    }

    /**
     * Validates a list of sidecar containers for conflicts with Strimzi internal definitions
     *
     * @param reconciliation        Reconciliation context
     * @param sidecarContainers    List of SidecarContainer objects to validate
     * @param userDefinedPorts     Set of user-defined listener ports to avoid conflicts
     * @param componentType        Component type for path generation (e.g., "kafka", "connect", "bridge")
     * @param reservedContainerNames Set of container names reserved by this component
     * @param reservedPorts        Set of port numbers reserved by this component
     * @throws InvalidResourceException if validation fails
     */
    public static void validateSidecarContainers(Reconciliation reconciliation, 
                                                List<SidecarContainer> sidecarContainers,
                                                Set<Integer> userDefinedPorts,
                                                String componentType,
                                                Set<String> reservedContainerNames,
                                                Set<Integer> reservedPorts) {
        if (sidecarContainers == null || sidecarContainers.isEmpty()) {
            return;
        }

        List<String> errors = new ArrayList<>();
        Set<String> containerNames = new HashSet<>();
        Set<Integer> usedPorts = new HashSet<>(COMMON_RESERVED_PORTS);
        
        // Add component-specific reserved ports
        if (reservedPorts != null) {
            usedPorts.addAll(reservedPorts);
        }
        
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
            validateContainerName(container, path, containerNames, reservedContainerNames, errors);
            
            // Validate port conflicts
            validateContainerPorts(container, path, usedPorts, errors);
            
            // Validate volume mount conflicts (only within same container)
            validateVolumeMounts(container, path, errors);
            
            // Validate resources
            validateResources(container, path, errors);
        }

        if (!errors.isEmpty()) {
            String errorMessage = "Sidecar container validation failed: " + String.join(", ", errors);
            LOGGER.errorCr(reconciliation, () -> errorMessage);
            throw new InvalidResourceException(errorMessage);
        }
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
            LOGGER.errorCr(reconciliation, () -> errorMessage);
            throw new InvalidResourceException(errorMessage);
        }
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
                                            Set<String> containerNames, Set<String> reservedContainerNames, List<String> errors) {
        String name = container.getName();
        if (name != null) {
            // Check for reserved names (if any are provided for this component)
            if (reservedContainerNames != null && reservedContainerNames.contains(name)) {
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
                    
                    // Check for duplicate ports within the pod (including component-reserved ports)
                    if (usedPorts.contains(portNumber)) {
                        errors.add(portPath + ".containerPort " + portNumber + " is reserved or already in use");
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

    /**
     * Converts sidecar containers from a pod template to Kubernetes Container objects.
     * This is a utility method for components to use in their interface implementations.
     *
     * @param templatePod     Pod template containing sidecar container definitions
     * @param imagePullPolicy Image pull policy to apply (currently unused but kept for compatibility)
     * @return List of converted Container objects
     */
    public static List<Container> convertSidecarContainers(
            PodTemplate templatePod, 
            ImagePullPolicy imagePullPolicy) {
        if (templatePod == null || templatePod.getSidecarContainers() == null) {
            return new ArrayList<>();
        }

        return ContainerUtils.convertSidecarContainers(templatePod.getSidecarContainers());
    }

    /**
     * Validates sidecar containers using the provided parameters.
     * This is a utility method for components to use in their interface implementations.
     * Automatically determines reserved elements based on component type.
     *
     * @param reconciliation       Reconciliation context
     * @param templatePod         Pod template containing sidecar containers
     * @param componentPorts      Set of ports used by the component that sidecars cannot use
     * @param componentType       Component type for error reporting (e.g., "kafka", "connect")
     */
    public static void validateSidecarContainersWithTemplate(Reconciliation reconciliation,
                                                            PodTemplate templatePod,
                                                            Set<Integer> componentPorts,
                                                            String componentType) {
        if (templatePod == null || templatePod.getSidecarContainers() == null) {
            return;
        }

        // Get component-specific reserved elements
        Set<String> reservedContainerNames = getReservedContainerNames(componentType);
        Set<Integer> reservedPorts = getReservedPorts(componentType);

        // Validate sidecar containers
        validateSidecarContainers(
                reconciliation,
                templatePod.getSidecarContainers(),
                componentPorts != null ? componentPorts : new HashSet<>(),
                componentType,
                reservedContainerNames,
                reservedPorts
        );

        // Validate volume references - extract volumes directly from template
        List<AdditionalVolume> additionalVolumes = templatePod.getVolumes();
        validateVolumeReferences(
                reconciliation,
                templatePod.getSidecarContainers(),
                additionalVolumes,
                componentType
        );
    }

    /**
     * Extracts all container ports from sidecar containers in a list of pod templates.
     * This is useful for components that need to know which ports are already in use by sidecars.
     *
     * @param podTemplates List of pod templates to check for sidecar containers
     * @return Set of port numbers used by all sidecar containers
     */
    public static Set<Integer> extractSidecarContainerPorts(List<? extends HasPodTemplate> podTemplates) {
        Set<Integer> sidecarPorts = new HashSet<>();
        
        if (podTemplates == null) {
            return sidecarPorts;
        }

        for (HasPodTemplate templateHolder : podTemplates) {
            PodTemplate templatePod = templateHolder.getPodTemplate();
            if (templatePod != null && templatePod.getSidecarContainers() != null) {
                for (SidecarContainer sidecar : templatePod.getSidecarContainers()) {
                    if (sidecar.getPorts() != null) {
                        for (ContainerPort port : sidecar.getPorts()) {
                            if (port.getContainerPort() != null) {
                                sidecarPorts.add(port.getContainerPort());
                            }
                        }
                    }
                }
            }
        }
        
        return sidecarPorts;
    }

    /**
     * Extracts all container ports from sidecar containers in a single pod template.
     *
     * @param templatePod Pod template to check for sidecar containers
     * @return Set of port numbers used by sidecar containers
     */
    public static Set<Integer> extractSidecarContainerPorts(PodTemplate templatePod) {
        if (templatePod == null) {
            return new HashSet<>();
        }
        
        return extractSidecarContainerPorts(List.of(new HasPodTemplate() {
            @Override
            public PodTemplate getPodTemplate() {
                return templatePod;
            }
        }));
    }

    /**
     * Interface for objects that contain a pod template.
     * This allows the helper to work with different types of objects that have pod templates.
     */
    public interface HasPodTemplate {
        PodTemplate getPodTemplate();
    }

    /**
     * Utility method to collect ports from various sources into a single set.
     * This makes it easy for components to build their required port set.
     *
     * @param portCollections Variable number of port collections to merge
     * @return Combined set of all ports from all collections
     */
    @SafeVarargs
    public static Set<Integer> collectPorts(Set<Integer>... portCollections) {
        Set<Integer> allPorts = new HashSet<>();
        for (Set<Integer> ports : portCollections) {
            if (ports != null) {
                allPorts.addAll(ports);
            }
        }
        return allPorts;
    }

    /**
     * Extracts ports from container port list.
     * Utility method to convert ContainerPort list to port number set.
     *
     * @param containerPorts List of container ports
     * @return Set of port numbers
     */
    public static Set<Integer> extractPortNumbers(List<ContainerPort> containerPorts) {
        Set<Integer> ports = new HashSet<>();
        if (containerPorts != null) {
            for (ContainerPort port : containerPorts) {
                if (port.getContainerPort() != null) {
                    ports.add(port.getContainerPort());
                }
            }
        }
        return ports;
    }
}
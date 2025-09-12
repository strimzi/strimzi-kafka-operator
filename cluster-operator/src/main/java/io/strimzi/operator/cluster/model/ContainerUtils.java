/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.ExecAction;
import io.fabric8.kubernetes.api.model.ExecActionBuilder;
import io.fabric8.kubernetes.api.model.HTTPGetActionBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Lifecycle;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.TCPSocketActionBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.SidecarContainer;
import io.strimzi.api.kafka.model.common.SidecarProbe;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared methods for working with Containers.
 */
public class ContainerUtils {
    private static final ReconciliationLogger LOGGER = 
            ReconciliationLogger.create(ContainerUtils.class);

    private ContainerUtils() {
        // Utility class, no instantiation
    }

    /**
     * Creates a container.
     *
     * @param name              Name of the container
     * @param containerImage    Container image
     * @param args              Arguments for starting the container
     * @param securityContext   Container security context
     * @param resources         Resource requirements
     * @param envVars           Environment variables
     * @param ports             Container ports
     * @param volumeMounts      Volume mounts
     * @param livenessProbe     Liveness Probe
     * @param readinessProbe    Readiness probe
     * @param imagePullPolicy   Desired image pull policy
     *
     * @return  New container
     */
    public static Container createContainer(
            final String name,
            final String containerImage,
            final List<String> args,
            final SecurityContext securityContext,
            final ResourceRequirements resources,
            final List<EnvVar> envVars,
            final List<ContainerPort> ports,
            final List<VolumeMount> volumeMounts,
            final Probe livenessProbe,
            final Probe readinessProbe,
            final ImagePullPolicy imagePullPolicy
    )   {
        return createContainer(
                name,
                containerImage,
                args,
                securityContext,
                resources,
                envVars,
                ports,
                volumeMounts,
                livenessProbe,
                readinessProbe,
                null,
                imagePullPolicy,
                null
        );
    }

    /**
     * Creates a container
     *
     * @param name              Name of the container
     * @param containerImage    Container image
     * @param args              Arguments for starting the container
     * @param securityContext   Container security context
     * @param resources         Resource requirements
     * @param envVars           Environment variables
     * @param ports             Container ports
     * @param volumeMounts      Volume mounts
     * @param livenessProbe     Liveness Probe
     * @param readinessProbe    Readiness probe
     * @param startupProbe      Startup probe
     * @param imagePullPolicy   Desired image pull policy
     * @param lifecycle         Container lifecycle policy
     *
     * @return  New container
     */
    public static Container createContainer(
            String name,
            String containerImage,
            List<String> args,
            SecurityContext securityContext,
            ResourceRequirements resources,
            List<EnvVar> envVars,
            List<ContainerPort> ports,
            List<VolumeMount> volumeMounts,
            Probe livenessProbe,
            Probe readinessProbe,
            Probe startupProbe,
            ImagePullPolicy imagePullPolicy,
            Lifecycle lifecycle
    )   {
        return new ContainerBuilder()
                .withName(name)
                .withImage(containerImage)
                .withEnv(envVars)
                .withVolumeMounts(volumeMounts)
                .withPorts(ports)
                .withLivenessProbe(livenessProbe)
                .withReadinessProbe(readinessProbe)
                .withStartupProbe(startupProbe)
                .withResources(resources)
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, containerImage))
                .withArgs(args)
                .withSecurityContext(securityContext)
                .withLifecycle(lifecycle)
                .build();
    }

    /**
     * Creates container port for exposing a port in a Pod. The protocol used is always TCP.
     *
     * @param name      Name of the pod.
     * @param port      Port number
     *
     * @return  Created container port
     */
    public static ContainerPort createContainerPort(String name, int port) {
        return new ContainerPortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withContainerPort(port)
                .build();
    }

    /**
     * Build an environment variable with the provided name and value
     *
     * @param name      The name of the environment variable
     * @param value     The value of the environment variable
     *
     * @return  The environment variable object
     */
    public static EnvVar createEnvVar(String name, String value) {
        return new EnvVarBuilder()
                .withName(name)
                .withValue(value)
                .build();
    }

    /**
     * Build an environment variable which will use a value from a secret
     *
     * @param name      The name of the environment variable
     * @param secret    The name of the secret where the value is stored
     * @param key       The key under which the value is stored in the secret
     *
     * @return  The environment variable object
     */
    public static EnvVar createEnvVarFromSecret(String name, String secret, String key) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(secret)
                        .withKey(key)
                    .endSecretKeyRef()
                .endValueFrom()
                .build();
    }

    /**
     * Build an environment variable which will use a value from a config map
     *
     * @param name      The name of the environment variable
     * @param configMap The name of the config map where the value is stored
     * @param key       The key under which the value is stored in the config map
     *
     * @return  The environment variable object
     */
    public static EnvVar createEnvVarFromConfigMap(String name, String configMap, String key) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withNewConfigMapKeyRef()
                        .withName(configMap)
                        .withKey(key)
                    .endConfigMapKeyRef()
                .endValueFrom()
                .build();
    }

    /**
     * Build an environment variable instance with the provided name from a field reference
     * using the Downward API.
     *
     * @param name      The name of the environment variable
     * @param field     The field path from which the value is set
     *
     * @return  The environment variable object
     */
    public static EnvVar createEnvVarFromFieldRef(String name, String field) {
        return new EnvVarBuilder()
                .withName(name)
                .withValueFrom(new EnvVarSourceBuilder()
                        .withNewFieldRef()
                            .withFieldPath(field)
                        .endFieldRef()
                        .build())
                .build();
    }

    /**
     * Adds the supplied list of user configured container environment variables {@see io.strimzi.api.kafka.model.common.template.ContainerEnvVar} to the
     * supplied list of fabric8 environment variables {@see io.fabric8.kubernetes.api.model.EnvVar},
     * checking first if the environment variable key has already been set in the existing list and then converts them.
     * <p>
     * If a key is already in use then the container environment variable will not be added to the environment variable
     * list and a warning will be logged.
     *
     * @param reconciliation    Reconciliation marker
     * @param existingEnvs      The list of fabric8 environment variable object that will be added to
     * @param template          Container template with user defined environment variables
     **/
    public static void addContainerEnvsToExistingEnvs(Reconciliation reconciliation, List<EnvVar> existingEnvs, ContainerTemplate template) {
        if (template != null && template.getEnv() != null) {
            // Create set of env var names to test if any user defined template env vars will conflict with those set above
            Set<String> alreadyUsedEnvNames = existingEnvs.stream().map(EnvVar::getName).collect(Collectors.toSet());

            // Set custom env vars from the user defined template
            for (ContainerEnvVar containerEnvVar : template.getEnv()) {
                if (alreadyUsedEnvNames.contains(containerEnvVar.getName())) {
                    LOGGER.warnCr(reconciliation, "User defined container template environment variable {} is already in use and will be ignored",  containerEnvVar.getName());
                } else if (containerEnvVar.getValue() != null) {
                    existingEnvs.add(createEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
                } else if (containerEnvVar.getValueFrom() != null && containerEnvVar.getValueFrom().getSecretKeyRef() != null) {
                    existingEnvs.add(createEnvVarFromSecret(containerEnvVar.getName(), 
                            containerEnvVar.getValueFrom().getSecretKeyRef().getName(), 
                            containerEnvVar.getValueFrom().getSecretKeyRef().getKey()));
                } else if (containerEnvVar.getValueFrom() != null && containerEnvVar.getValueFrom().getConfigMapKeyRef() != null) {
                    existingEnvs.add(createEnvVarFromConfigMap(containerEnvVar.getName(), 
                            containerEnvVar.getValueFrom().getConfigMapKeyRef().getName(), 
                            containerEnvVar.getValueFrom().getConfigMapKeyRef().getKey()));
                } else {
                    LOGGER.warnCr(reconciliation, "User defined container template environment variable {} doesn't have any value defined and will be ignored",  containerEnvVar.getName());
                }
            }
        }
    }

    /**
     * When ImagePullPolicy is not specified by the user, Kubernetes will automatically set it based on the image
     *    :latest results in        Always
     *    anything else results in  IfNotPresent
     * This causes issues in diffing. To work around this we emulate here the Kubernetes defaults and set the policy
     * accordingly on our side.
     * <p>
     * This is applied to the Strimzi Kafka images which use the tag format :latest-kafka-x.y.z but have the same function
     * as if they were :latest. Therefore they should behave the same with an ImagePullPolicy of Always. This also emulates
     * the behavior expected from users based on the Kubernetes defaults.
     *
     * @param imagePullPolicy  The imagePullPolicy requested by the user (is always preferred when set, ignored when null)
     * @param containerImage   The image used for the container, from its tag we determine the default policy if requestedImagePullPolicy is null
     *
     * @return  The Image Pull Policy: Always, Never or IfNotPresent
     */
    /* test */ static String determineImagePullPolicy(ImagePullPolicy imagePullPolicy, String containerImage)  {
        if (imagePullPolicy != null)   {
            // A specific image pull policy was requested => we will honor it
            return imagePullPolicy.toString();
        } else if (containerImage.toLowerCase(Locale.ENGLISH).contains(":latest"))  {
            // No policy was requested => we use Always for latest images
            return ImagePullPolicy.ALWAYS.toString();
        } else {
            // No policy was requested => we use IfNotPresent for latest images
            return ImagePullPolicy.IFNOTPRESENT.toString();
        }
    }

    /**
     * Utility method for wrapping a container into a list. It either returns null if the container passed as argument
     * is null or a list with the container if it is not null. This helps to make sure a list of init containers - which
     * sometimes exist and sometimes not depending on the settings - is passed correctly. If you would just wrap it in
     * List.of(), it would not accept the null value.
     *
     * @param container     Container which should be wrapped into list
     *
     * @return  List with the container or null if the container is null
     */
    public static List<Container> listOrNull(Container container)   {
        return container != null ? List.of(container) : null;
    }

    /**
     * Converts a list of SidecarContainers to a list of Fabric8 Containers
     *
     * @param sidecarContainers the list of SidecarContainers to convert
     * @return the converted list of Containers
     */
    public static List<Container> convertSidecarContainers(List<SidecarContainer> sidecarContainers) {
        if (sidecarContainers == null) {
            return new ArrayList<>();
        }

        return sidecarContainers.stream()
                .map(ContainerUtils::convertSidecarContainer)
                .collect(Collectors.toList());
    }

    /**
     * Converts a list of SidecarContainers to a list of Fabric8 Containers with image pull policy
     *
     * @param sidecarContainers the list of SidecarContainers to convert
     * @param imagePullPolicy the image pull policy to apply if not set on individual containers
     * @return the converted list of Containers
     */
    public static List<Container> convertSidecarContainers(List<SidecarContainer> sidecarContainers, ImagePullPolicy imagePullPolicy) {
        if (sidecarContainers == null) {
            return new ArrayList<>();
        }

        return sidecarContainers.stream()
                .map(sidecar -> convertSidecarContainer(sidecar, imagePullPolicy))
                .collect(Collectors.toList());
    }

    /**
     * Converts a SidecarContainer to a Fabric8 Container
     *
     * @param sidecarContainer the SidecarContainer to convert
     * @return the converted Container
     */
    public static Container convertSidecarContainer(SidecarContainer sidecarContainer) {
        if (sidecarContainer == null) {
            return null;
        }

        // Convert ports - can use directly since ContainerPort doesn't have IntOrString issues
        List<ContainerPort> ports = sidecarContainer.getPorts();

        // Convert environment variables
        List<EnvVar> envVars = new ArrayList<>();
        if (sidecarContainer.getEnv() != null) {
            for (ContainerEnvVar containerEnvVar : sidecarContainer.getEnv()) {
                if (containerEnvVar.getValue() != null) {
                    envVars.add(createEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
                } else if (containerEnvVar.getValueFrom() != null && containerEnvVar.getValueFrom().getSecretKeyRef() != null) {
                    envVars.add(createEnvVarFromSecret(containerEnvVar.getName(), 
                            containerEnvVar.getValueFrom().getSecretKeyRef().getName(), 
                            containerEnvVar.getValueFrom().getSecretKeyRef().getKey()));
                } else if (containerEnvVar.getValueFrom() != null && containerEnvVar.getValueFrom().getConfigMapKeyRef() != null) {
                    envVars.add(createEnvVarFromConfigMap(containerEnvVar.getName(), 
                            containerEnvVar.getValueFrom().getConfigMapKeyRef().getName(), 
                            containerEnvVar.getValueFrom().getConfigMapKeyRef().getKey()));
                }
            }
        }

        // Convert probes
        Probe livenessProbe = convertSidecarProbe(sidecarContainer.getLivenessProbe());
        Probe readinessProbe = convertSidecarProbe(sidecarContainer.getReadinessProbe());

        // Handle image pull policy - convert string to enum
        ImagePullPolicy imagePullPolicyEnum = null;
        if (sidecarContainer.getImagePullPolicy() != null) {
            String policy = sidecarContainer.getImagePullPolicy();
            switch (policy) {
                case "Always":
                    imagePullPolicyEnum = ImagePullPolicy.ALWAYS;
                    break;
                case "IfNotPresent":
                    imagePullPolicyEnum = ImagePullPolicy.IFNOTPRESENT;
                    break;
                case "Never":
                    imagePullPolicyEnum = ImagePullPolicy.NEVER;
                    break;
                default:
                    // Leave as null for invalid values
                    break;
            }
        }
        
        return new ContainerBuilder()
                .withName(sidecarContainer.getName())
                .withImage(sidecarContainer.getImage())
                .withCommand(sidecarContainer.getCommand())
                .withArgs(sidecarContainer.getArgs())
                .withSecurityContext(sidecarContainer.getSecurityContext())
                .withResources(sidecarContainer.getResources())
                .withEnv(envVars)
                .withPorts(ports)
                .withVolumeMounts(sidecarContainer.getVolumeMounts())
                .withLivenessProbe(livenessProbe)
                .withReadinessProbe(readinessProbe)
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicyEnum, sidecarContainer.getImage()))
                .build();
    }

    /**
     * Converts a SidecarContainer to a Fabric8 Container with image pull policy
     *
     * @param sidecarContainer the SidecarContainer to convert
     * @param defaultImagePullPolicy the default image pull policy to apply if not set
     * @return the converted Container
     */
    public static Container convertSidecarContainer(SidecarContainer sidecarContainer, ImagePullPolicy defaultImagePullPolicy) {
        if (sidecarContainer == null) {
            return null;
        }

        // Convert ports - can use directly since ContainerPort doesn't have IntOrString issues
        List<ContainerPort> ports = sidecarContainer.getPorts();

        // Convert environment variables
        List<EnvVar> envVars = new ArrayList<>();
        if (sidecarContainer.getEnv() != null) {
            for (ContainerEnvVar containerEnvVar : sidecarContainer.getEnv()) {
                if (containerEnvVar.getValue() != null) {
                    envVars.add(createEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
                } else if (containerEnvVar.getValueFrom() != null && containerEnvVar.getValueFrom().getSecretKeyRef() != null) {
                    envVars.add(createEnvVarFromSecret(containerEnvVar.getName(), 
                            containerEnvVar.getValueFrom().getSecretKeyRef().getName(), 
                            containerEnvVar.getValueFrom().getSecretKeyRef().getKey()));
                } else if (containerEnvVar.getValueFrom() != null && containerEnvVar.getValueFrom().getConfigMapKeyRef() != null) {
                    envVars.add(createEnvVarFromConfigMap(containerEnvVar.getName(), 
                            containerEnvVar.getValueFrom().getConfigMapKeyRef().getName(), 
                            containerEnvVar.getValueFrom().getConfigMapKeyRef().getKey()));
                }
            }
        }

        // Convert probes
        Probe livenessProbe = convertSidecarProbe(sidecarContainer.getLivenessProbe());
        Probe readinessProbe = convertSidecarProbe(sidecarContainer.getReadinessProbe());

        // Handle image pull policy - prefer sidecar-specific setting, fall back to default
        ImagePullPolicy imagePullPolicyEnum = null;
        if (sidecarContainer.getImagePullPolicy() != null) {
            String policy = sidecarContainer.getImagePullPolicy();
            switch (policy) {
                case "Always":
                    imagePullPolicyEnum = ImagePullPolicy.ALWAYS;
                    break;
                case "IfNotPresent":
                    imagePullPolicyEnum = ImagePullPolicy.IFNOTPRESENT;
                    break;
                case "Never":
                    imagePullPolicyEnum = ImagePullPolicy.NEVER;
                    break;
                default:
                    // Leave as null for invalid values
                    break;
            }
        }

        // Use default if no specific policy set
        if (imagePullPolicyEnum == null && defaultImagePullPolicy != null) {
            String policyString = determineImagePullPolicy(defaultImagePullPolicy, sidecarContainer.getImage());
            // Convert string back to enum
            switch (policyString) {
                case "Always":
                    imagePullPolicyEnum = ImagePullPolicy.ALWAYS;
                    break;
                case "IfNotPresent":
                    imagePullPolicyEnum = ImagePullPolicy.IFNOTPRESENT;
                    break;
                case "Never":
                    imagePullPolicyEnum = ImagePullPolicy.NEVER;
                    break;
                default:
                    // Leave as null for invalid values
                    break;
            }
        }
        
        return new ContainerBuilder()
                .withName(sidecarContainer.getName())
                .withImage(sidecarContainer.getImage())
                .withCommand(sidecarContainer.getCommand())
                .withArgs(sidecarContainer.getArgs())
                .withSecurityContext(sidecarContainer.getSecurityContext())
                .withResources(sidecarContainer.getResources())
                .withEnv(envVars)
                .withPorts(ports)
                .withVolumeMounts(sidecarContainer.getVolumeMounts())
                .withLivenessProbe(livenessProbe)
                .withReadinessProbe(readinessProbe)
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicyEnum, sidecarContainer.getImage()))
                .build();
    }

    /**
     * Converts a SidecarProbe to a Fabric8 Probe
     *
     * @param sidecarProbe the SidecarProbe to convert
     * @return the converted Probe
     */
    private static Probe convertSidecarProbe(SidecarProbe sidecarProbe) {
        if (sidecarProbe == null) {
            return null;
        }

        ProbeBuilder builder = new ProbeBuilder()
                .withInitialDelaySeconds(sidecarProbe.getInitialDelaySeconds())
                .withTimeoutSeconds(sidecarProbe.getTimeoutSeconds())
                .withPeriodSeconds(sidecarProbe.getPeriodSeconds())
                .withSuccessThreshold(sidecarProbe.getSuccessThreshold())
                .withFailureThreshold(sidecarProbe.getFailureThreshold());

        // Convert exec action
        if (sidecarProbe.getExecCommand() != null && !sidecarProbe.getExecCommand().isEmpty()) {
            ExecAction execAction = new ExecActionBuilder()
                    .withCommand(sidecarProbe.getExecCommand())
                    .build();
            builder.withExec(execAction);
        }

        // Convert HTTP GET action
        if (sidecarProbe.getHttpGetPath() != null || sidecarProbe.getHttpGetPort() != null) {
            HTTPGetActionBuilder httpBuilder = new HTTPGetActionBuilder()
                    .withPath(sidecarProbe.getHttpGetPath())
                    .withScheme(sidecarProbe.getHttpGetScheme());

            if (sidecarProbe.getHttpGetPort() != null) {
                try {
                    int port = Integer.parseInt(sidecarProbe.getHttpGetPort());
                    httpBuilder.withPort(new IntOrString(port));
                } catch (NumberFormatException e) {
                    // Named port
                    httpBuilder.withPort(new IntOrString(sidecarProbe.getHttpGetPort()));
                }
            }

            builder.withHttpGet(httpBuilder.build());
        }

        // Convert TCP socket action
        if (sidecarProbe.getTcpSocketPort() != null) {
            TCPSocketActionBuilder tcpBuilder = new TCPSocketActionBuilder();

            try {
                int port = Integer.parseInt(sidecarProbe.getTcpSocketPort());
                tcpBuilder.withPort(new IntOrString(port));
            } catch (NumberFormatException e) {
                // Named port
                tcpBuilder.withPort(new IntOrString(sidecarProbe.getTcpSocketPort()));
            }

            builder.withTcpSocket(tcpBuilder.build());
        }

        return builder.build();
    }
}

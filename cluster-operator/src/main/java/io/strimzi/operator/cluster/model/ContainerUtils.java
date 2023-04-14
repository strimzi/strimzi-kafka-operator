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
import io.fabric8.kubernetes.api.model.Lifecycle;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.Reconciliation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Shared methods for working with Containers
 */
public class ContainerUtils {
    /**
     * Configure statically defined environment variables which are passed to all operands.
     * This includes HTTP/HTTPS Proxy env vars or the FIPS_MODE.
     */
    private static final List<EnvVar> STATIC_ENV_VARS;
    static {
        List<EnvVar> envVars = new ArrayList<>();

        if (System.getenv(ClusterOperatorConfig.HTTP_PROXY) != null)    {
            envVars.add(createEnvVar(ClusterOperatorConfig.HTTP_PROXY, System.getenv(ClusterOperatorConfig.HTTP_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.HTTPS_PROXY) != null)    {
            envVars.add(createEnvVar(ClusterOperatorConfig.HTTPS_PROXY, System.getenv(ClusterOperatorConfig.HTTPS_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.NO_PROXY) != null)    {
            envVars.add(createEnvVar(ClusterOperatorConfig.NO_PROXY, System.getenv(ClusterOperatorConfig.NO_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.FIPS_MODE) != null)    {
            envVars.add(createEnvVar(ClusterOperatorConfig.FIPS_MODE, System.getenv(ClusterOperatorConfig.FIPS_MODE)));
        }

        if (envVars.size() > 0) {
            STATIC_ENV_VARS = Collections.unmodifiableList(envVars);
        } else {
            STATIC_ENV_VARS = List.of();
        }
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
     * @param imagePullPolicy   Desired image pull policy
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
            ImagePullPolicy imagePullPolicy
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
     * Adds the supplied list of user configured container environment variables {@see io.strimzi.api.kafka.model.ContainerEnvVar} to the
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
            Set<String> predefinedEnvs = new HashSet<>();
            for (EnvVar envVar : existingEnvs) {
                predefinedEnvs.add(envVar.getName());
            }

            // Set custom env vars from the user defined template
            for (ContainerEnvVar containerEnvVar : template.getEnv()) {
                if (predefinedEnvs.contains(containerEnvVar.getName())) {
                    AbstractModel.LOGGER.warnCr(reconciliation, "User defined container template environment variable {} is already in use and will be ignored",  containerEnvVar.getName());
                } else {
                    existingEnvs.add(createEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
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
     * Returns a lit of environment variables which are required by all containers. These contain things such as FIPS
     * configurations or HTTP Proxy configurations.
     *
     * @return  List of required environment variables for all containers
     */
    public static List<EnvVar> requiredEnvVars() {
        return STATIC_ENV_VARS;
    }
}

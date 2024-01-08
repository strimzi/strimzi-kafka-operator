/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.jmx;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.strimzi.api.kafka.model.common.jmx.HasJmxOptions;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.operator.cluster.model.ContainerUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NetworkPolicyUtils;
import io.strimzi.operator.cluster.model.ServiceUtils;
import io.strimzi.operator.cluster.model.TemplateUtils;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a model for components with enabled JMX access
 */
public class JmxModel {
    /**
     * Name of the port used for JMX
     */
    public static final String JMX_PORT_NAME = "jmx";

    /**
     * Port number used for JMX
     */
    public static final int JMX_PORT = 9999;

    /**
     * Key under which the JMX username is stored in the JMX Secret
     */
    public static final String JMX_USERNAME_KEY = "jmx-username";

    /**
     * Key under which the JMX password is stored in the JMX Secret
     */
    public static final String JMX_PASSWORD_KEY = "jmx-password";

    /**
     * Name of the environment variable used to indicate that JMX should be enabled in the container
     */
    public static final String ENV_VAR_STRIMZI_JMX_ENABLED = "STRIMZI_JMX_ENABLED";

    /**
     * Name of the environment variable used for the JMX username. This variable is mapped from the JMX Secret.
     */
    public static final String ENV_VAR_STRIMZI_JMX_USERNAME = "STRIMZI_JMX_USERNAME";

    /**
     * Name of the environment variable used for the JMX password. This variable is mapped from the JMX Secret.
     */
    public static final String ENV_VAR_STRIMZI_JMX_PASSWORD = "STRIMZI_JMX_PASSWORD";

    private final boolean isEnabled;
    private final boolean isAuthenticated;

    private final String namespace;
    private final String secretName;
    private final Labels labels;
    private final OwnerReference ownerReference;

    private final ResourceTemplate template;

    /**
     * Constructs the JMX Model for managing JMX access to Strimzi
     *
     * @param namespace         Namespace of the cluster with JMX access
     * @param secretName        Name of the JMX Secret
     * @param labels            Labels for the JMX Secret
     * @param ownerReference    Owner reference for the JMX Secret
     * @param specSection       Custom resource section configuring the JMX access
     */
    public JmxModel(String namespace, String secretName, Labels labels, OwnerReference ownerReference, HasJmxOptions specSection) {
        this.namespace = namespace;
        this.secretName = secretName;
        this.labels = labels;
        this.ownerReference = ownerReference;

        if (specSection.getJmxOptions() != null)    {
            this.isEnabled = true;
            this.isAuthenticated = specSection.getJmxOptions().getAuthentication() != null
                    && specSection.getJmxOptions().getAuthentication() instanceof KafkaJmxAuthenticationPassword;
            this.template = specSection.getTemplate() != null ? specSection.getTemplate().getJmxSecret() : null;
        } else {
            this.isEnabled = false;
            this.isAuthenticated = false;
            this.template = null;
        }
    }

    /**
     * @return  The name of the JMX Secret
     */
    public String secretName()   {
        return secretName;
    }

    /**
     * @return  Returns a JMX Service port if JMX is enabled. Returns empty list otherwise.
     */
    public List<ServicePort> servicePorts()    {
        if (isEnabled)  {
            return List.of(ServiceUtils.createServicePort(JMX_PORT_NAME, JMX_PORT, JMX_PORT, "TCP"));
        } else {
            return List.of();
        }
    }

    /**
     * @return  Returns a JMX Container port if JMX is enabled. Returns empty list otherwise.
     */
    public List<ContainerPort> containerPorts()    {
        if (isEnabled)  {
            return List.of(ContainerUtils.createContainerPort(JMX_PORT_NAME, JMX_PORT));
        } else {
            return List.of();
        }
    }

    /**
     * @return  Returns a list of JMX environment variables if JMX is enabled. Returns empty list otherwise.
     */
    public List<EnvVar> envVars()   {
        List<EnvVar> envVars = new ArrayList<>();

        if (isEnabled) {
            envVars.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_JMX_ENABLED, "true"));

            if (isAuthenticated) {
                envVars.add(ContainerUtils.createEnvVarFromSecret(ENV_VAR_STRIMZI_JMX_USERNAME, secretName, JMX_USERNAME_KEY));
                envVars.add(ContainerUtils.createEnvVarFromSecret(ENV_VAR_STRIMZI_JMX_PASSWORD, secretName, JMX_PASSWORD_KEY));
            }
        }

        return envVars;
    }

    /**
     * @return  Returns a JMX network policy ingress rule if JMX is enabled. Returns empty list otherwise.
     */
    public List<NetworkPolicyIngressRule> networkPolicyIngresRules()   {
        if (isEnabled)  {
            return List.of(NetworkPolicyUtils.createIngressRule(JMX_PORT, List.of()));
        } else {
            return List.of();
        }
    }

    /**
     * Generates the JMX Secret containing the username and password to secure the JMX port.
     *
     * @param currentSecret     The existing JMX Secret with the current JMX credentials. Null if no secret exists yet.
     *
     * @return  The generated JMX Secret
     */
    public Secret jmxSecret(Secret currentSecret) {
        if (isEnabled && isAuthenticated) {
            PasswordGenerator passwordGenerator = new PasswordGenerator(16);
            Map<String, String> data = new HashMap<>(2);

            if (currentSecret != null && currentSecret.getData() != null)  {
                data.put(JMX_USERNAME_KEY, currentSecret.getData().computeIfAbsent(JMX_USERNAME_KEY, (key) -> Util.encodeToBase64(passwordGenerator.generate())));
                data.put(JMX_PASSWORD_KEY, currentSecret.getData().computeIfAbsent(JMX_PASSWORD_KEY, (key) -> Util.encodeToBase64(passwordGenerator.generate())));
            } else {
                data.put(JMX_USERNAME_KEY, Util.encodeToBase64(passwordGenerator.generate()));
                data.put(JMX_PASSWORD_KEY, Util.encodeToBase64(passwordGenerator.generate()));
            }

            return ModelUtils.createSecret(
                    secretName,
                    namespace,
                    labels,
                    ownerReference,
                    data,
                    TemplateUtils.annotations(template),
                    TemplateUtils.labels(template)
            );
        } else {
            return null;
        }
    }
}
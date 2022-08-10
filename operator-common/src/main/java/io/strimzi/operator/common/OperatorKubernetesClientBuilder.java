/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

/**
 * Class for generating Kubernetes Clients for Operators.
 */
public class OperatorKubernetesClientBuilder {

    private final String componentName;
    private final String version;


    /**
     * Constructor to create the OperatorKubernetesClientBuilder with the name of the component and its version.
     *
     * @param componentName  The name of the component using the client.
     * @param version        The version of the component using the client.
     */
    public OperatorKubernetesClientBuilder(final String componentName, final String version) {
        this.componentName = componentName;
        this.version = version;
    }

    /**
     * Builds the KubernetesClient.
     *
     * @return the Kubernetes Client
     */
    public KubernetesClient build() {
        final String userAgent = String.format("%s/%s", componentName, version);
        final Config kubernetesClientConfig = new ConfigBuilder().withUserAgent(userAgent).build();
        return new KubernetesClientBuilder().withConfig(kubernetesClientConfig).build();
    }
}

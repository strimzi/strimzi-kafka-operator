/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper.impl;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.plugin.gatekeeper.GatekeeperPluginConfigurationContext;

/**
 * Default implementation of the {@link GatekeeperPluginConfigurationContext} passed to the plugins when they are
 * configured at operator startup.
 */
public class GatekeeperPluginConfigurationContextImpl implements GatekeeperPluginConfigurationContext {
    private final KubernetesClient kubernetesClient;

    /**
     * Creates the configuration context.
     *
     * @param kubernetesClient  Kubernetes client which the plugins can use to interact with the Kubernetes API
     */
    public GatekeeperPluginConfigurationContextImpl(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    @Override
    public KubernetesClient kubernetesClient() {
        return kubernetesClient;
    }

}

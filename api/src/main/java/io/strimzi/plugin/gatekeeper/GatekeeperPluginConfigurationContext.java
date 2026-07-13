/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Context passed to {@link GatekeeperPlugin#configure(GatekeeperPluginConfigurationContext)} when a plugin is
 * initialized at operator startup. It provides the resources the plugin can use to configure itself.
 */
public interface GatekeeperPluginConfigurationContext {
    /**
     * Returns the Kubernetes client which the plugin can use to interact with the Kubernetes API.
     *
     * @return  Kubernetes client
     */
    KubernetesClient kubernetesClient();
}

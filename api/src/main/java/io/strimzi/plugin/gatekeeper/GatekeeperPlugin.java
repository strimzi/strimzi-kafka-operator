/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

/**
 * Base interface implemented by all Strimzi Gatekeeper plugins. A plugin hooks into the reconciliation of one or more
 * Strimzi operands. To do so, it also implements one or more of the type-specific mutating
 * ({@code Gatekeeper<Type>MutatingPlugin}) or validating ({@code Gatekeeper<Type>ValidatingPlugin}) interfaces.
 * <p>
 * Plugins are loaded once at operator startup and initialized through their {@link #configure(GatekeeperPluginConfigurationContext)}
 * method.
 */
public interface GatekeeperPlugin {
    /**
     * Configures the plugin. This method is called exactly once at operator startup, before any of the entry or exit
     * methods are invoked. It can be used to preconfigure the plugin based on the platform it is running on or based on
     * information from additional sources (for example, environment variables). If the configuration fails with an
     * exception, the operator startup fails.
     *
     * @param context   Context providing the resources (such as the Kubernetes client and the platform features) the
     *                  plugin can use to configure itself
     */
    default void configure(GatekeeperPluginConfigurationContext context) { }
}

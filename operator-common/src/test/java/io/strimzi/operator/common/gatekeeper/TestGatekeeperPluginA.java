/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper;

import io.strimzi.plugin.gatekeeper.GatekeeperPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperPluginConfigurationContext;

/**
 * Test Gatekeeper plugin used to verify the ServiceLoader discovery, ordering and configuration done by the factory.
 */
public class TestGatekeeperPluginA implements GatekeeperPlugin {
    private int configureCount = 0;
    private GatekeeperPluginConfigurationContext context;

    @Override
    public void configure(GatekeeperPluginConfigurationContext context) {
        this.configureCount++;
        this.context = context;
    }

    public int configureCount() {
        return configureCount;
    }

    public GatekeeperPluginConfigurationContext context() {
        return context;
    }
}

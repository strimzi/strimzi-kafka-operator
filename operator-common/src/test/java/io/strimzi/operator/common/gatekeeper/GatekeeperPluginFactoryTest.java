/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.plugin.gatekeeper.GatekeeperPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperPluginConfigurationContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class GatekeeperPluginFactoryTest {
    private static final String PLUGIN_A = TestGatekeeperPluginA.class.getCanonicalName();
    private static final String PLUGIN_B = TestGatekeeperPluginB.class.getCanonicalName();
    private static final String UNKNOWN_PLUGIN = "io.strimzi.does.not.Exist";

    private final GatekeeperPluginConfigurationContext context = mock(GatekeeperPluginConfigurationContext.class);

    @AfterEach
    public void cleanup() {
        // Reset the static state and remove any thread-local plugins so the tests do not interfere with each other.
        GatekeeperPluginFactory.initialize(List.of(), context);
        GatekeeperPluginFactory.clearTestPlugins();
    }

    private static List<String> classNames(List<? extends GatekeeperPlugin> plugins) {
        return plugins.stream().map(p -> p.getClass().getCanonicalName()).toList();
    }

    @Test
    public void testInitializeKeepsRequestedOrder() {
        GatekeeperPluginFactory.initialize(List.of(PLUGIN_A, PLUGIN_B), context);

        assertThat(classNames(GatekeeperPluginFactory.getPlugins()), contains(PLUGIN_A, PLUGIN_B));

        // Different order -> B first, A second
        GatekeeperPluginFactory.initialize(List.of(PLUGIN_B, PLUGIN_A), context);

        assertThat(classNames(GatekeeperPluginFactory.getPlugins()), contains(PLUGIN_B, PLUGIN_A));
    }

    @Test
    public void testInitializeLoadsOnlyRequestedPlugins() {
        GatekeeperPluginFactory.initialize(List.of(PLUGIN_B), context);

        assertThat(classNames(GatekeeperPluginFactory.getPlugins()), contains(PLUGIN_B));
    }

    @Test
    public void testInitializeConfiguresEachPluginOnceWithContext() {
        GatekeeperPluginFactory.initialize(List.of(PLUGIN_A, PLUGIN_B), context);

        List<GatekeeperPlugin> plugins = GatekeeperPluginFactory.getPlugins();
        TestGatekeeperPluginA pluginA = (TestGatekeeperPluginA) plugins.get(0);
        TestGatekeeperPluginB pluginB = (TestGatekeeperPluginB) plugins.get(1);

        assertThat(pluginA.configureCount(), is(1));
        assertThat(pluginB.configureCount(), is(1));
        assertThat(pluginA.context(), is(sameInstance(context)));
        assertThat(pluginB.context(), is(sameInstance(context)));
    }

    @Test
    public void testInitializeWithEmptyList() {
        GatekeeperPluginFactory.initialize(List.of(), context);

        assertThat(GatekeeperPluginFactory.getPlugins(), is(List.of()));
    }

    @Test
    public void testInitializeWithUnknownPluginThrows() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> GatekeeperPluginFactory.initialize(List.of(PLUGIN_A, UNKNOWN_PLUGIN), context));

        assertThat(e.getMessage(), is("Gatekeeper plugin " + UNKNOWN_PLUGIN + " was not found."));
    }

    @Test
    public void testInitializeForTestsOverridesStaticPluginsViaThreadLocal() {
        // Configure the static plugins
        GatekeeperPluginFactory.initialize(List.of(PLUGIN_A), context);
        assertThat(classNames(GatekeeperPluginFactory.getPlugins()), contains(PLUGIN_A));

        // The thread-local plugins take precedence over the static ones
        GatekeeperPlugin threadLocalPlugin = new TestGatekeeperPluginB();
        GatekeeperPluginFactory.initializeForTests(List.of(threadLocalPlugin));
        assertThat(GatekeeperPluginFactory.getPlugins(), contains(sameInstance(threadLocalPlugin)));

        // Once the thread-local plugins are cleared, the static ones are visible again
        GatekeeperPluginFactory.clearTestPlugins();
        assertThat(classNames(GatekeeperPluginFactory.getPlugins()), contains(PLUGIN_A));
    }

    @Test
    public void testInitializeForTestsDoesNotConfigurePlugins() {
        TestGatekeeperPluginA plugin = new TestGatekeeperPluginA();
        GatekeeperPluginFactory.initializeForTests(List.of(plugin));

        assertThat(GatekeeperPluginFactory.getPlugins(), contains(sameInstance(plugin)));
        assertThat(plugin.configureCount(), is(0));
    }
}

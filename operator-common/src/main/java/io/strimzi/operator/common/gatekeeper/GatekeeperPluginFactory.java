/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.plugin.gatekeeper.GatekeeperPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperPluginConfigurationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory that loads, configures, and holds the Strimzi Gatekeeper plugins used by the operator. Plugins are found and
 * instantiated using the Java {@link ServiceLoader} mechanism based on the {@link GatekeeperPlugin} interface.
 * <p>
 * The plugins are stored as an ordered list. The order is significant: it is preserved exactly as requested during the
 * initialization and is later used when the plugins are invoked during reconciliations.
 * <p>
 * The factory holds the plugins in a static field for the regular operator runtime. For tests, the
 * {@link #initializeForTests(List)} method stores the plugins in a thread-local field instead, so that tests running in
 * parallel can inject their own plugins without interfering with each other or with the static state.
 */
public class GatekeeperPluginFactory {
    private static final Logger LOGGER = LogManager.getLogger(GatekeeperPluginFactory.class);

    // Plugins configured for the operator process. Defaults to an empty list so that it is safe to use the factory
    // before it is initialized (for example, in unit tests which do not use any plugins).
    private static volatile List<GatekeeperPlugin> plugins = List.of();

    // Thread-local override used by tests. When set, it takes precedence over the static field. This allows tests
    // running in parallel to use their own set of plugins without affecting other threads.
    private static final ThreadLocal<List<GatekeeperPlugin>> TEST_PLUGINS = new ThreadLocal<>();

    private GatekeeperPluginFactory() { }

    /**
     * Initializes the factory for the regular operator runtime. It finds the requested plugins using the
     * {@link ServiceLoader} mechanism, configures them by calling their
     * {@link GatekeeperPlugin#configure(GatekeeperPluginConfigurationContext)} method and stores them in the order in
     * which they are listed. The order is preserved and is significant for the order in which the plugins are invoked.
     * <p>
     * If any of the requested plugins is not found, or if configuring a plugin fails, an exception is thrown and the
     * operator startup is expected to fail.
     *
     * @param pluginClasses     Ordered list of the canonical class names of the plugins which should be used
     * @param context           Configuration context passed to each plugin when it is configured
     */
    public static void initialize(List<String> pluginClasses, GatekeeperPluginConfigurationContext context) {
        plugins = loadConfigureAndOrder(pluginClasses, context);
    }

    /**
     * Initializes the factory for tests. Unlike {@link #initialize(List, GatekeeperPluginConfigurationContext)}, it does
     * not use the {@link ServiceLoader} mechanism and does not configure the plugins. Instead, it stores the provided
     * ordered list of plugin instances in a thread-local field which is visible only to the current thread. This lets
     * tests inject their own (e.g., mocked) plugins and run in parallel without interfering with each other.
     * <p>
     * Tests should call {@link #clearTestPlugins()} when they are done to avoid leaking the thread-local value.
     *
     * @param plugins   Ordered list of plugin instances to be used by the current thread
     */
    public static void initializeForTests(List<GatekeeperPlugin> plugins) {
        TEST_PLUGINS.set(plugins == null ? List.of() : List.copyOf(plugins));
    }

    /**
     * Removes the thread-local plugins set by {@link #initializeForTests(List)}. Tests should call this in their
     * teardown to avoid leaking the thread-local value into other tests reusing the same thread.
     */
    public static void clearTestPlugins() {
        TEST_PLUGINS.remove();
    }

    /**
     * Returns the ordered list of all configured plugins. If a thread-local list of plugins was set using
     * {@link #initializeForTests(List)}, it takes precedence over the plugins configured for the regular runtime.
     *
     * @return  Ordered, unmodifiable list of the configured plugins
     */
    public static List<GatekeeperPlugin> getPlugins() {
        List<GatekeeperPlugin> testPlugins = TEST_PLUGINS.get();
        return testPlugins != null ? testPlugins : plugins;
    }

    /**
     * Finds the requested plugins using the ServiceLoader mechanism, orders them as requested, configures them,
     * and returns them as an unmodifiable list.
     *
     * @param pluginClasses     Ordered list of the canonical class names of the plugins which should be used
     * @param context           Configuration context passed to each plugin when it is configured
     *
     * @return  Ordered, unmodifiable list of the configured plugins
     */
    private static List<GatekeeperPlugin> loadConfigureAndOrder(List<String> pluginClasses, GatekeeperPluginConfigurationContext context) {
        if (pluginClasses == null || pluginClasses.isEmpty()) {
            return List.of();
        } else {
            // Discover all the available plugins at once and index them by their canonical class name.
            Map<String, GatekeeperPlugin> available = new LinkedHashMap<>();
            for (GatekeeperPlugin plugin : ServiceLoader.load(GatekeeperPlugin.class)) {
                LOGGER.info("Found Gatekeeper plugin {}", plugin.getClass().getCanonicalName());
                available.put(plugin.getClass().getCanonicalName(), plugin);
            }

            // Resolve the requested plugins keeping the requested order, which is significant.
            List<GatekeeperPlugin> ordered = new ArrayList<>(pluginClasses.size());
            for (String pluginClass : pluginClasses) {
                GatekeeperPlugin plugin = available.get(pluginClass);

                if (plugin == null) {
                    LOGGER.error("Gatekeeper plugin {} was not found", pluginClass);
                    throw new InvalidConfigurationException("Gatekeeper plugin " + pluginClass + " was not found.");
                } else {
                    ordered.add(plugin);
                }
            }

            // Configure the plugins in order. If configuring a plugin throws, it is propagated and the operator startup fails.
            for (GatekeeperPlugin plugin : ordered) {
                LOGGER.info("Configuring Gatekeeper plugin {}", plugin.getClass().getCanonicalName());
                plugin.configure(context);
            }

            // Return unmodifiable list of ordered plugins
            return List.copyOf(ordered);
        }
    }
}

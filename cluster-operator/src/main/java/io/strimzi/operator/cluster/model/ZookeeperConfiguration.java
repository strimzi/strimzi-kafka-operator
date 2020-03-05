/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.ZookeeperClusterSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Class for handling Zookeeper configuration passed by the user
 */
public class ZookeeperConfiguration extends AbstractConfiguration {

    private static final List<String> FORBIDDEN_OPTIONS;
    protected static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = new ArrayList<>();
        FORBIDDEN_OPTIONS.addAll(Arrays.asList(ZookeeperClusterSpec.FORBIDDEN_PREFIXES.split(" *, *")));
        // This option is handled in the Zookeeper container startup script
        FORBIDDEN_OPTIONS.add("snapshot.trust.empty");
        // This option would prevent scaling beyond 1 node for clusters started with a single node
        FORBIDDEN_OPTIONS.add("standaloneEnabled");
        // Reconfiguration needs to be enabled to allow scaling of the cluster
        FORBIDDEN_OPTIONS.add("reconfigEnabled");
        // The Cluster Operator requires access to multiple 4LW and access to the nodes is secured by the TLS-Sidecars so we set all allowed
        FORBIDDEN_OPTIONS.add("4lw.commands.whitelist");

        Map<String, String> config = new HashMap<>();
        config.put("tickTime", "2000");
        config.put("initLimit", "5");
        config.put("syncLimit", "2");
        config.put("autopurge.purgeInterval", "1");
        DEFAULTS = Collections.unmodifiableMap(config);
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public ZookeeperConfiguration(String configuration) {
        this(configuration, FORBIDDEN_OPTIONS);
    }

    /**
     * Constructor used to instantiate this class from String configuration and validated against the supplied forbidden
     * options.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     * @param forbiddenOptions List of option names that are not allowed to be set.
     */
    public ZookeeperConfiguration(String configuration, List<String> forbiddenOptions) {
        super(configuration, forbiddenOptions, DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public ZookeeperConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, DEFAULTS);
    }

    /**
     * Returns a ZookeeperConfiguration created without forbidden option filtering.
     * @param string A string representation of the Properties
     * @return The ZookeeperConfiguration
     */
    public static ZookeeperConfiguration unvalidated(String string) {
        return new ZookeeperConfiguration(string, emptyList());
    }

}

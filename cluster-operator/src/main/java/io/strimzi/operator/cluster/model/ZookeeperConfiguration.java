/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.Zookeeper;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;

/**
 * Class for handling Zookeeper configuration passed by the user
 */
public class ZookeeperConfiguration extends AbstractConfiguration {

    private static final List<String> FORBIDDEN_OPTIONS;
    private static final Properties DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(
                Zookeeper.FORBIDDEN_PREFIXES.split(" *, *"));

        DEFAULTS = new Properties();
        DEFAULTS.setProperty("timeTick", "2000");
        DEFAULTS.setProperty("initLimit", "5");
        DEFAULTS.setProperty("syncLimit", "2");
        DEFAULTS.setProperty("autopurge.purgeInterval", "1");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public ZookeeperConfiguration(String configuration) {
        super(configuration, FORBIDDEN_OPTIONS, DEFAULTS);
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
}

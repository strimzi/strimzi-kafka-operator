/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.ZookeeperClusterSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Class for handling Zookeeper configuration passed by the user
 */
public class ZookeeperConfiguration extends AbstractConfiguration {

    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> EXCEPTIONS;
    protected static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(ZookeeperClusterSpec.FORBIDDEN_PREFIXES.split(", "));
        EXCEPTIONS = asList(ZookeeperClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS.split(", "));

        Map<String, String> config = new HashMap<>();
        config.put("tickTime", "2000");
        config.put("initLimit", "5");
        config.put("syncLimit", "2");
        config.put("autopurge.purgeInterval", "1");
        DEFAULTS = Collections.unmodifiableMap(config);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public ZookeeperConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, EXCEPTIONS, DEFAULTS);
    }
}

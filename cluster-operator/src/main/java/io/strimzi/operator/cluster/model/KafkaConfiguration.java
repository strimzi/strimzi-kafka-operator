/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;
    private static final Properties DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(
                "listeners",
                "advertised.",
                "broker.",
                "listener.",
                "host.name",
                "port",
                "inter.broker.listener.name",
                "sasl.",
                "ssl.",
                "security.",
                "password.",
                "principal.builder.class",
                "log.dir",
                "zookeeper.connect",
                "zookeeper.set.acl",
                "authorizer.",
                "super.user");

        DEFAULTS = new Properties();
        DEFAULTS.setProperty("controlled.shutdown.enable", "true");
        DEFAULTS.setProperty("auto.leader.rebalance.enable", "true");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public KafkaConfiguration(String configuration) {
        super(configuration, FORBIDDEN_OPTIONS, DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(JsonObject jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, DEFAULTS);
    }
}

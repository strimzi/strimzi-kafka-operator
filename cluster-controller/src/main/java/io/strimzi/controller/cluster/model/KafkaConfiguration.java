/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.controller.cluster.model;

import io.vertx.core.json.JsonObject;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;

    static {
        FORBIDDEN_OPTIONS = asList(
                "listeners",
                "advertised.listeners",
                "broker.id",
                "listener.",
                "inter.broker.listener.name",
                "sasl.",
                "ssl.",
                "log.dirs",
                "zookeeper.connect",
                "authorizer.",
                "super.user");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public KafkaConfiguration(String configuration) {
        super(configuration, FORBIDDEN_OPTIONS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(JsonObject jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS);
    }
}

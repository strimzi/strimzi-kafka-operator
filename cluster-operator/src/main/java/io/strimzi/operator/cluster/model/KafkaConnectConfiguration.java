/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.KafkaConnectAssemblySpec;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;

/**
 * Class for handling Kafka Connect configuration passed by the user
 */
public class KafkaConnectConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;
    private static final Properties DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(KafkaConnectAssemblySpec.FORBIDDEN_PREFIXES.split(", "));

        DEFAULTS = new Properties();
        DEFAULTS.setProperty("group.id", "connect-cluster");
        DEFAULTS.setProperty("offset.storage.topic", "connect-cluster-offsets");
        DEFAULTS.setProperty("config.storage.topic", "connect-cluster-configs");
        DEFAULTS.setProperty("status.storage.topic", "connect-cluster-status");
        DEFAULTS.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        DEFAULTS.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        DEFAULTS.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        DEFAULTS.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        DEFAULTS.setProperty("internal.key.converter.schemas.enable", "false");
        DEFAULTS.setProperty("internal.value.converter.schemas.enable", "false");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public KafkaConnectConfiguration(String configuration) {
        super(configuration, FORBIDDEN_OPTIONS, DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConnectConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, DEFAULTS);
    }
}

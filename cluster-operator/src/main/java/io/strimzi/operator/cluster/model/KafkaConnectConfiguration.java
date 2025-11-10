/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.operator.common.Reconciliation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for handling Kafka Connect configuration passed by the user
 */
public class KafkaConnectConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaConnectSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaConnectSpec.FORBIDDEN_PREFIX_EXCEPTIONS);

        DEFAULTS = new HashMap<>(2);
        DEFAULTS.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        DEFAULTS.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    }

    /**
     * Copy constructor which creates a new instance of the Kafka Connect configuration from an existing configuration.
     * It is useful when you need to modify an instance of the configuration without permanently changing the original.
     *
     * @param configuration User provided Kafka Connect configuration
     *
     */
    public KafkaConnectConfiguration(KafkaConnectConfiguration configuration) {
        super(configuration);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     JSON object with configuration options as key ad value pairs.
     */
    public KafkaConnectConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS, List.of(), DEFAULTS);
    }
}

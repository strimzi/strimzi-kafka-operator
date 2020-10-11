/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.KafkaBridgeConsumerSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Class for handling Kafka Bridge consumer configuration passed by the user
 */
public class KafkaBridgeConsumerConfiguration extends AbstractConfiguration {

    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> EXCEPTIONS;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(KafkaBridgeConsumerSpec.FORBIDDEN_PREFIXES.split(", "));
        EXCEPTIONS = asList(KafkaBridgeConsumerSpec.FORBIDDEN_PREFIX_EXCEPTIONS.split(", "));
        DEFAULTS = new HashMap<>();
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaBridgeConsumerConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, EXCEPTIONS, DEFAULTS);
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationCustom;
import io.strimzi.operator.common.Reconciliation;

import java.util.List;
import java.util.Map;

/**
 * Class for handling Kafka listener configuration passed by the user
 */
public class KafkaListenerCustomAuthConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_PREFIXES;
    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesToList(KafkaListenerAuthenticationCustom.FORBIDDEN_PREFIXES);
    }

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param jsonOptions       Configuration options
     */
    public KafkaListenerCustomAuthConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES);
    }
}

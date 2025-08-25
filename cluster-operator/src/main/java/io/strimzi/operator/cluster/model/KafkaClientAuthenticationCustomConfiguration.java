/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationCustom;
import io.strimzi.operator.common.Reconciliation;

import java.util.List;
import java.util.Map;

/**
 * Class for handling configuration of the custom client-side authentication
 */
public class KafkaClientAuthenticationCustomConfiguration extends AbstractConfiguration {
    private static final List<String> ALLOWED_PREFIXES;
    static {
        ALLOWED_PREFIXES = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaClientAuthenticationCustom.ALLOWED_PREFIXES);
    }

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param jsonOptions       Configuration options
     */
    public KafkaClientAuthenticationCustomConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, ALLOWED_PREFIXES);
    }
}

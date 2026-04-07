/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpec;
import io.strimzi.operator.common.Reconciliation;

import java.util.List;
import java.util.Map;

/**
 * Class for handling Kafka Bridge configuration passed by the user
 */
public class KafkaBridgeConfiguration extends AbstractConfiguration {

    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_OPTIONS;

    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaBridgeSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_OPTIONS = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaBridgeSpec.FORBIDDEN_OPTIONS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaBridgeConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, List.of(), FORBIDDEN_OPTIONS, Map.of());
    }
}

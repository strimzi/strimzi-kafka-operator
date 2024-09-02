/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.connector.AbstractConnectorSpec;
import io.strimzi.operator.common.Reconciliation;

import java.util.List;
import java.util.Map;

/**
 * Class for handling KafkaConnector configuration passed by the user
 */
public class KafkaConnectorConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;

    static {
        FORBIDDEN_OPTIONS = AbstractConfiguration.splitPrefixesOrOptionsToList(AbstractConnectorSpec.FORBIDDEN_PARAMETERS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConnectorConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, List.of(), List.of(), FORBIDDEN_OPTIONS, Map.of());
    }
}

/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Class for handling Kafka Bridge producer configuration passed by the user
 */
public class KafkaBridgeProducerConfiguration extends AbstractConfiguration {

    private static final Map<String, String> DEFAULTS;

    static {

        DEFAULTS = new HashMap<>();
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaBridgeProducerConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, Collections.EMPTY_LIST, DEFAULTS);
    }
}

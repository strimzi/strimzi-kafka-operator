/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.KafkaClusterSpec;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {

    public static final String INTERBROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";
    public static final String LOG_MESSAGE_FORMAT_VERSION = "log.message.format.version";

    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> EXCEPTIONS;

    static {
        FORBIDDEN_OPTIONS = asList(KafkaClusterSpec.FORBIDDEN_PREFIXES.split(", "));
        EXCEPTIONS = asList("zookeeper.connection.timeout.ms");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public KafkaConfiguration(String configuration) {
        this(configuration, FORBIDDEN_OPTIONS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, EXCEPTIONS);
    }

    private KafkaConfiguration(String configuration, List<String> forbiddenOptions) {
        super(configuration, forbiddenOptions);
    }

    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     * @param string A string representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaConfiguration unvalidated(String string) {
        return new KafkaConfiguration(string, emptyList());
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.CruiseControlSpec;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class for handling Cruise Control configuration passed by the user
 */
public class CruiseControlConfiguration extends AbstractConfiguration {

    /**
     * A list of case insensitive goals that Cruise Control supports in the order of priority.
     * The high priority goals will be executed first.
     */
    protected static final List<String> CRUISE_CONTROL_GOALS_LIST = Collections.unmodifiableList(
        Arrays.asList(
                CruiseControl.RACK_AWARENESS_GOAL,
                CruiseControl.REPLICA_CAPACITY_GOAL,
                CruiseControl.DISK_CAPACITY_GOAL,
                CruiseControl.NETWORK_INBOUND_CAPACITY_GOAL,
                CruiseControl.NETWORK_OUTBOUND_CAPACITY_GOAL,
                CruiseControl.CPU_CAPACITY_GOAL,
                CruiseControl.REPLICA_DISTRIBUTION_GOAL,
                CruiseControl.POTENTIAL_NETWORK_OUTAGE_GOAL,
                CruiseControl.DISK_USAGE_DISTRIBUTION_GOAL,
                CruiseControl.NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL,
                CruiseControl.NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL,
                CruiseControl.CPU_USAGE_DISTRIBUTION_GOAL,
                CruiseControl.TOPIC_REPLICA_DISTRIBUTION_GOAL,
                CruiseControl.LEADER_REPLICA_DISTRIBUTION_GOAL,
                CruiseControl.LEADER_BYTES_IN_DISTRIBUTION_GOAL,
                CruiseControl.PREFERRED_LEADER_ELECTION_GOAL
        )
     );

    public static final String CRUISE_CONTROL_GOALS = String.join(",", CRUISE_CONTROL_GOALS_LIST);

    protected static final List<String> CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS_LIST = Collections.unmodifiableList(
        Arrays.asList(
                CruiseControl.RACK_AWARENESS_GOAL,
                CruiseControl.REPLICA_CAPACITY_GOAL,
                CruiseControl.DISK_CAPACITY_GOAL
        )
    );

    public static final String CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS =
            String.join(",", CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS_LIST);

    public static final String CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY = "default.goals";
    public static final String CRUISE_CONTROL_SELF_HEALING_CONFIG_KEY = "self.healing.goals";
    public static final String CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY = "anomaly.detection.goals";

   /*
    * Map containing default values for required configuration properties
    */
    private static final Map<String, String> CRUISE_CONTROL_DEFAULT_PROPERTIES_MAP;

    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS;

    static {
        Map<String, String> config = new HashMap<>(7);
        config.put("partition.metrics.window.ms", Integer.toString(300_000));
        config.put("num.partition.metrics.windows", "1");
        config.put("broker.metrics.window.ms", Integer.toString(300_000));
        config.put("num.broker.metrics.windows", "20");
        config.put("completed.user.task.retention.time.ms", Long.toString(TimeUnit.DAYS.toMillis(1)));
        config.put("default.goals", CRUISE_CONTROL_GOALS);
        config.put("goals", CRUISE_CONTROL_GOALS);
        CRUISE_CONTROL_DEFAULT_PROPERTIES_MAP = Collections.unmodifiableMap(config);

        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesToList(CruiseControlSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesToList(CruiseControlSpec.FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public CruiseControlConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    private CruiseControlConfiguration(String configuration, List<String> forbiddenPrefixes) {
        super(configuration, forbiddenPrefixes);
    }

    public static Map<String, String> getCruiseControlDefaultPropertiesMap() {
        return CRUISE_CONTROL_DEFAULT_PROPERTIES_MAP;
    }
}

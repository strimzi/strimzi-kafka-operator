/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlGoals;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.Reconciliation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Class for handling Cruise Control configuration passed by the user
 */
public class CruiseControlConfiguration extends AbstractConfiguration {
    /**
     * A list of case insensitive goals that Cruise Control supports in the order of priority.
     * The high priority goals will be executed first.
     */
    protected static final List<String> CRUISE_CONTROL_GOALS_LIST = List.of(
            CruiseControlGoals.RACK_AWARENESS_GOAL.toString(),
            CruiseControlGoals.MIN_TOPIC_LEADERS_PER_BROKER_GOAL.toString(),
            CruiseControlGoals.REPLICA_CAPACITY_GOAL.toString(),
            CruiseControlGoals.DISK_CAPACITY_GOAL.toString(),
            CruiseControlGoals.NETWORK_INBOUND_CAPACITY_GOAL.toString(),
            CruiseControlGoals.NETWORK_OUTBOUND_CAPACITY_GOAL.toString(),
            CruiseControlGoals.CPU_CAPACITY_GOAL.toString(),
            CruiseControlGoals.REPLICA_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.POTENTIAL_NETWORK_OUTAGE_GOAL.toString(),
            CruiseControlGoals.DISK_USAGE_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.CPU_USAGE_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.TOPIC_REPLICA_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.LEADER_REPLICA_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.LEADER_BYTES_IN_DISTRIBUTION_GOAL.toString(),
            CruiseControlGoals.PREFERRED_LEADER_ELECTION_GOAL.toString()
    );

    protected static final String CRUISE_CONTROL_GOALS = String.join(",", CRUISE_CONTROL_GOALS_LIST);

    /**
     * A list of case insensitive goals that Cruise Control will use as hard goals that must all be met for an optimization
     * proposal to be valid.
     */
    protected static final List<String> CRUISE_CONTROL_HARD_GOALS_LIST = List.of(
            CruiseControlGoals.RACK_AWARENESS_GOAL.toString(),
            CruiseControlGoals.REPLICA_CAPACITY_GOAL.toString(),
            CruiseControlGoals.DISK_CAPACITY_GOAL.toString(),
            CruiseControlGoals.NETWORK_INBOUND_CAPACITY_GOAL.toString(),
            CruiseControlGoals.NETWORK_OUTBOUND_CAPACITY_GOAL.toString(),
            CruiseControlGoals.CPU_CAPACITY_GOAL.toString()
    );

    private static final String CRUISE_CONTROL_HARD_GOALS = String.join(",", CRUISE_CONTROL_HARD_GOALS_LIST);

    protected static final List<String> CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS_LIST = List.of(
            CruiseControlGoals.RACK_AWARENESS_GOAL.toString(),
            CruiseControlGoals.MIN_TOPIC_LEADERS_PER_BROKER_GOAL.toString(),
            CruiseControlGoals.REPLICA_CAPACITY_GOAL.toString(),
            CruiseControlGoals.DISK_CAPACITY_GOAL.toString()
    );

    protected static final String CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS =
            String.join(",", CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS_LIST);

    /*
    * Map containing default values for required configuration properties. The map needs to be sorted so that the order
    * of the entries in the Cruise Control configuration is deterministic and does not cause unnecessary rolling updates
    * of Cruise Control deployment.
    */
    private static final Map<String, String> CRUISE_CONTROL_DEFAULT_PROPERTIES_MAP = Collections.unmodifiableSortedMap(new TreeMap<>(Map.ofEntries(
            Map.entry(CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_MS_CONFIG_KEY.getValue(), Integer.toString(300_000)),
            Map.entry(CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY.getValue(), "1"),
            Map.entry(CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_MS_CONFIG_KEY.getValue(), Integer.toString(300_000)),
            Map.entry(CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_NUM_CONFIG_KEY.getValue(), "20"),
            Map.entry(CruiseControlConfigurationParameters.COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY.getValue(), Long.toString(TimeUnit.DAYS.toMillis(1))),
            Map.entry(CruiseControlConfigurationParameters.GOALS_CONFIG_KEY.getValue(), CRUISE_CONTROL_GOALS),
            Map.entry(CruiseControlConfigurationParameters.DEFAULT_GOALS_CONFIG_KEY.getValue(), CRUISE_CONTROL_GOALS),
            Map.entry(CruiseControlConfigurationParameters.HARD_GOALS_CONFIG_KEY.getValue(), CRUISE_CONTROL_HARD_GOALS),
            Map.entry(CruiseControlConfigurationParameters.WEBSERVER_SECURITY_ENABLE.getValue(), Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SECURITY_ENABLED)),
            Map.entry(CruiseControlConfigurationParameters.WEBSERVER_AUTH_CREDENTIALS_FILE.getValue(), CruiseControl.API_AUTH_CREDENTIALS_FILE),
            Map.entry(CruiseControlConfigurationParameters.WEBSERVER_SSL_ENABLE.getValue(), Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SSL_ENABLED)),
            Map.entry(CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_PARTITION_METRIC_TOPIC_NAME),
            Map.entry(CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_BROKER_METRIC_TOPIC_NAME),
            Map.entry(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME)
    )));

    private static final List<String> FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesToList(CruiseControlSpec.FORBIDDEN_PREFIXES);
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesToList(CruiseControlSpec.FORBIDDEN_PREFIX_EXCEPTIONS);

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public CruiseControlConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    protected static Map<String, String> getCruiseControlDefaultPropertiesMap() {
        return CRUISE_CONTROL_DEFAULT_PROPERTIES_MAP;
    }

    private boolean isEnabledInConfiguration(String s1, String s2) {
        String s = getConfigOption(s1, s2);
        return Boolean.parseBoolean(s);
    }

    /**
     * @return  True if API authentication is enabled. False otherwise.
     */
    public boolean isApiAuthEnabled() {
        return isEnabledInConfiguration(CruiseControlConfigurationParameters.WEBSERVER_SECURITY_ENABLE.getValue(), Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SECURITY_ENABLED));
    }

    /**
     * @return  True if TLS is enabled. False otherwise.
     */
    public boolean isApiSslEnabled() {
        return isEnabledInConfiguration(CruiseControlConfigurationParameters.WEBSERVER_SSL_ENABLE.getValue(), Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SSL_ENABLED));
    }
}

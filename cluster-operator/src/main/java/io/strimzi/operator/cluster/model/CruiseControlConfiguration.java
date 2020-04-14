/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.CruiseControlSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Class for handling Cruise Control configuration passed by the user
 */
public class CruiseControlConfiguration extends AbstractConfiguration {

    /**
     * A list of case insensitive goals that Cruise Control supports in the order of priority.
     * The high priority goals will be executed first.
     */
    private static final String DEFAULT_GOALS = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal," +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal" +
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal";

   /*
    * Map containing default values for required configuration properties
    */
    private static final Map<String, String> CC_DEFAULT_PROPERTIES_MAP;

    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> EXCEPTIONS;

    static {
        CC_DEFAULT_PROPERTIES_MAP = new HashMap<>();
        CC_DEFAULT_PROPERTIES_MAP.put("partition.metrics.window.ms", Integer.toString(300_000));
        CC_DEFAULT_PROPERTIES_MAP.put("num.partition.metrics.windows", "1");
        CC_DEFAULT_PROPERTIES_MAP.put("broker.metrics.window.ms", Integer.toString(300_000));
        CC_DEFAULT_PROPERTIES_MAP.put("num.broker.metrics.windows", "20");
        CC_DEFAULT_PROPERTIES_MAP.put("completed.user.task.retention.time.ms", Long.toString(TimeUnit.DAYS.toMillis(1)));
        CC_DEFAULT_PROPERTIES_MAP.put("default.goals", DEFAULT_GOALS);
        CC_DEFAULT_PROPERTIES_MAP.put("goals", DEFAULT_GOALS);

        FORBIDDEN_OPTIONS = asList(CruiseControlSpec.FORBIDDEN_PREFIXES.split(", "));
        EXCEPTIONS = asList(CruiseControlSpec.FORBIDDEN_PREFIX_EXCEPTIONS.split(", "));
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public CruiseControlConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, EXCEPTIONS);
    }

    private CruiseControlConfiguration(String configuration, List<String> forbiddenOptions) {
        super(configuration, forbiddenOptions);
    }

    /**
     * Returns a CruiseControlConfiguration created without forbidden option filtering.
     * @param string A string representation of the Properties
     * @return The CruiseControlConfiguration
     */
    public static CruiseControlConfiguration unvalidated(String string) {
        return new CruiseControlConfiguration(string, emptyList());
    }

    public static Map<String, String> getCruiseControlDefaultPropertiesMap() {
        return CC_DEFAULT_PROPERTIES_MAP;
    }
}

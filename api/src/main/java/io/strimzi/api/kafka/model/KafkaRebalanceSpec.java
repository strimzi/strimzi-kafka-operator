/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "goals", "skipHardGoalCheck", "excludedTopics", "concurrentPartitionMovementsPerBroker",
                     "concurrentIntraBrokerPartitionMovements", "concurrentLeaderMovements", "replicationThrottle", "replicaMovementStrategies" })
@EqualsAndHashCode
public class KafkaRebalanceSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    // Optimization goal configurations
    private List<String> goals;
    private boolean skipHardGoalCheck;

    // Topic configuration
    private String excludedTopics;

    // Rebalance performance tuning configurations
    private int concurrentPartitionMovementsPerBroker;
    private int concurrentIntraBrokerPartitionMovements;
    private int concurrentLeaderMovements;
    private long replicationThrottle;
    private List<String> replicaMovementStrategies;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("A list of goals, ordered by decreasing priority, to use for generating and executing the rebalance proposal. " +
            "The supported goals are available at https://github.com/linkedin/cruise-control#goals. " +
            "If an empty goals list is provided, the goals declared in the default.goals Cruise Control configuration parameter are used.")
    public List<String> getGoals() {
        return goals;
    }

    public void setGoals(List<String> goals) {
        this.goals = goals;
    }

    @Description("Whether to allow the hard goals specified in the Kafka CR to be skipped in optimization proposal generation. " +
            "This can be useful when some of those hard goals are preventing a balance solution being found. " +
            "Default is false.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isSkipHardGoalCheck() {
        return skipHardGoalCheck;
    }

    public void setSkipHardGoalCheck(boolean skipHardGoalCheck) {
        this.skipHardGoalCheck = skipHardGoalCheck;
    }

    @Description("A regular expression where any matching topics will be excluded from the calculation of optimization proposals. " +
            "This expression will be parsed by the java.util.regex.Pattern class; for more information on the supported formar " +
            "consult the documentation for that class.")
    public String getExcludedTopics() {
        return excludedTopics;
    }

    public void setExcludedTopics(String excludedTopics) {
        this.excludedTopics = excludedTopics;
    }

    @Description("The upper bound of ongoing partition replica movements going into/out of each broker. Default is 5.")
    @Minimum(0)
    public int getConcurrentPartitionMovementsPerBroker() {
        return concurrentPartitionMovementsPerBroker;
    }

    public void setConcurrentPartitionMovementsPerBroker(int movements) {
        this.concurrentPartitionMovementsPerBroker = movements;
    }

    @Description("The upper bound of ongoing partition replica movements between disks within each broker. Default is 2.")
    @Minimum(0)
    public int getConcurrentIntraBrokerPartitionMovements() {
        return concurrentIntraBrokerPartitionMovements;
    }

    public void setConcurrentIntraBrokerPartitionMovements(int movements) {
        this.concurrentIntraBrokerPartitionMovements = movements;
    }

    @Description("The upper bound of ongoing partition leadership movements. Default is 1000.")
    @Minimum(0)
    public int getConcurrentLeaderMovements() {
        return concurrentLeaderMovements;
    }

    public void setConcurrentLeaderMovements(int movements) {
        this.concurrentLeaderMovements = movements;
    }

    @Description("The upper bound, in bytes per second, on the bandwidth used to move replicas. There is no limit by default.")
    @Minimum(0)
    public long getReplicationThrottle() {
        return replicationThrottle;
    }

    public void setReplicationThrottle(long bandwidth) {
        this.replicationThrottle = bandwidth;
    }

    @Description("A list of strategy class names used to determine the execution order for the replica movements in the generated optimization proposal. " +
        "By default BaseReplicaMovementStrategy is used, which will execute the replica movements in the order that they were generated.")
    public List<String> getReplicaMovementStrategies() {
        return replicaMovementStrategies;
    }

    public void setReplicaMovementStrategies(List<String> replicaMovementStrategies) {
        this.replicaMovementStrategies = replicaMovementStrategies;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
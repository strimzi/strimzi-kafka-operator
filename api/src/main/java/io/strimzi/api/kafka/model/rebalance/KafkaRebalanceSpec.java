/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "mode", "brokers", "goals", "skipHardGoalCheck", "rebalanceDisk", "excludedTopics", "concurrentPartitionMovementsPerBroker",
    "concurrentIntraBrokerPartitionMovements", "concurrentLeaderMovements", "replicationThrottle", "replicaMovementStrategies" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaRebalanceSpec extends Spec {
    private static final long serialVersionUID = 1L;

    // rebalancing modes
    private KafkaRebalanceMode mode = KafkaRebalanceMode.FULL;
    private List<Integer> brokers;

    // Optimization goal configurations
    private List<String> goals;
    private boolean skipHardGoalCheck;
    private boolean rebalanceDisk;

    // Topic configuration
    private String excludedTopics;

    // Rebalance performance tuning configurations
    private int concurrentPartitionMovementsPerBroker;
    private int concurrentIntraBrokerPartitionMovements;
    private int concurrentLeaderMovements;
    private long replicationThrottle;
    private List<String> replicaMovementStrategies;

    @Description("Mode to run the rebalancing. " +
            "The supported modes are `full`, `add-brokers`, `remove-brokers`.\n" +
            "If not specified, the `full` mode is used by default. \n\n" +
            "* `full` mode runs the rebalancing across all the brokers in the cluster.\n" +
            "* `add-brokers` mode can be used after scaling up the cluster to move some replicas to the newly added brokers.\n" +
            "* `remove-brokers` mode can be used before scaling down the cluster to move replicas out of the brokers to be removed.\n")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public KafkaRebalanceMode getMode() {
        return mode;
    }

    public void setMode(KafkaRebalanceMode mode) {
        this.mode = mode;
    }

    @Description("The list of newly added brokers in case of scaling up or the ones to be removed in case of scaling down to use for rebalancing. " +
            "This list can be used only with rebalancing mode `add-brokers` and `removed-brokers`. It is ignored with `full` mode.")
    public List<Integer> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<Integer> brokers) {
        this.brokers = brokers;
    }

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

    @Description("Enables intra-broker disk balancing, which balances disk space utilization between disks on the same broker. " +
            "Only applies to Kafka deployments that use JBOD storage with multiple disks. " +
            "When enabled, inter-broker balancing is disabled. Default is false.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isRebalanceDisk() {
        return rebalanceDisk;
    }

    public void setRebalanceDisk(boolean rebalanceDisk) {
        this.rebalanceDisk = rebalanceDisk;
    }

    @Description("A regular expression where any matching topics will be excluded from the calculation of optimization proposals. " +
            "This expression will be parsed by the java.util.regex.Pattern class; for more information on the supported format " +
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
}

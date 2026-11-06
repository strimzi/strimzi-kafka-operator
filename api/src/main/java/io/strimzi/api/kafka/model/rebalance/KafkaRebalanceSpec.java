/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.crdgenerator.annotations.DeprecatedProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "mode", "nodes", "config", "brokers", "goals", "skipHardGoalCheck", "rebalanceDisk", "excludedTopics", "concurrentPartitionMovementsPerBroker",
    "concurrentIntraBrokerPartitionMovements", "concurrentLeaderMovements", "replicationThrottle", "replicaMovementStrategies", "moveReplicasOffVolumes" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaRebalanceSpec extends Spec {

    /**
     * Config keys that are managed by the operator and must not be set in {@code spec.config}.
     * These are filtered at reconciliation time with a warning.
     */
    public static final String FORBIDDEN_KEYS = "dryrun, json, verbose, super_verbose, brokerid, brokerid_and_logdirs";

    // rebalancing modes
    private KafkaRebalanceMode mode = KafkaRebalanceMode.FULL;

    private List<RebalanceNode> nodes;

    private Map<String, String> config;

    // Deprecated fields
    private List<Integer> brokers;
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
    private List<RebalanceNode> moveReplicasOffVolumes;

    @Description("Mode to run the rebalancing. " +
            "The supported modes are `full`, `add-brokers`, `remove-brokers`.\n" +
            "If not specified, the `full` mode is used by default. \n\n" +
            "* `full` mode runs the rebalancing across all the brokers in the cluster.\n" +
            "* `add-brokers` mode can be used after scaling up the cluster to move some replicas to the newly added brokers.\n" +
            "* `remove-brokers` mode can be used before scaling down the cluster to move replicas out of the brokers to be removed.\n" +
            "* `remove-disks` mode can be used to move data across the volumes within the same broker\n")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public KafkaRebalanceMode getMode() {
        return mode;
    }

    public void setMode(KafkaRebalanceMode mode) {
        this.mode = mode;
    }

    @Description("List of target brokers and, optionally, specific volumes on those brokers for the rebalance operation. " +
            "For broker-targeting modes (`add-brokers`, `remove-brokers`), only `brokerId` is specified and `volumeIds` must not be present. " +
            "For volume-targeting modes (`remove-disks`), both `brokerId` and `volumeIds` are required. " +
            "The `nodes` field is not supported in `full` mode.")
    @MinimumItems(1)
    public List<RebalanceNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<RebalanceNode> nodes) {
        this.nodes = nodes;
    }

    @Description("Cruise Control REST API configuration parameters for tuning the rebalance operation. " +
            "Keys use upstream Cruise Control parameter names (e.g. `concurrent_partition_movements_per_broker`, `excluded_topics`, `goals`). " +
            "Values are strings that will be passed directly to the Cruise Control REST API. " +
            "Parameters managed by the operator (" + FORBIDDEN_KEYS + ") are forbidden and will be ignored with a warning. " +
            "See the [Cruise Control REST API documentation](https://github.com/cruise-control-for-kafka/cruise-control/wiki/Configurations) for available parameters.")
    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.nodes", removalVersion = "v2")
    @Description("The list of newly added brokers in case of scaling up or the ones to be removed in case of scaling down to use for rebalancing. " +
            "This list can be used only with rebalancing mode `add-brokers` and `removed-brokers`. It is ignored with `full` mode. " +
            "This configuration has been deprecated, use `spec.nodes` instead.")
    public List<Integer> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<Integer> brokers) {
        this.brokers = brokers;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("A list of goals, ordered by decreasing priority, to use for generating and executing the rebalance proposal. " +
            "The supported goals are available at https://github.com/linkedin/cruise-control#goals. " +
            "If an empty goals list is provided, the goals declared in the default.goals Cruise Control configuration parameter are used. " +
            "This configuration has been deprecated, use `spec.config` with key `goals` instead.")
    public List<String> getGoals() {
        return goals;
    }

    public void setGoals(List<String> goals) {
        this.goals = goals;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("Whether to allow the hard goals specified in the Kafka CR to be skipped in optimization proposal generation. " +
            "This can be useful when some of those hard goals are preventing a balance solution being found. " +
            "Default is false. " +
            "This configuration has been deprecated, use `spec.config` with key `skip_hard_goal_check` instead.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isSkipHardGoalCheck() {
        return skipHardGoalCheck;
    }

    public void setSkipHardGoalCheck(boolean skipHardGoalCheck) {
        this.skipHardGoalCheck = skipHardGoalCheck;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("Enables intra-broker disk balancing, which balances disk space utilization between disks on the same broker. " +
            "Only applies to Kafka deployments that use JBOD storage with multiple disks. " +
            "When enabled, inter-broker balancing is disabled. Default is false. " +
            "This configuration has been deprecated, use `spec.config` with key `rebalance_disk` instead.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isRebalanceDisk() {
        return rebalanceDisk;
    }

    public void setRebalanceDisk(boolean rebalanceDisk) {
        this.rebalanceDisk = rebalanceDisk;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @SuppressWarnings("checkstyle:NoFullyQualifiedClassNames") // False positive, fully qualified class name used in a string
    @Description("A regular expression where any matching topics will be excluded from the calculation of optimization proposals. " +
            "This expression will be parsed by the java.util.regex.Pattern class; for more information on the supported format " +
            "consult the documentation for that class. " +
            "This configuration has been deprecated, use `spec.config` with key `excluded_topics` instead.")
    public String getExcludedTopics() {
        return excludedTopics;
    }

    public void setExcludedTopics(String excludedTopics) {
        this.excludedTopics = excludedTopics;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("The upper bound of ongoing partition replica movements going into/out of each broker. Default is 5. " +
            "This configuration has been deprecated, use `spec.config` with key `concurrent_partition_movements_per_broker` instead.")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getConcurrentPartitionMovementsPerBroker() {
        return concurrentPartitionMovementsPerBroker;
    }

    public void setConcurrentPartitionMovementsPerBroker(int movements) {
        this.concurrentPartitionMovementsPerBroker = movements;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("The upper bound of ongoing partition replica movements between disks within each broker. Default is 2. " +
            "This configuration has been deprecated, use `spec.config` with key `concurrent_intra_broker_partition_movements` instead.")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getConcurrentIntraBrokerPartitionMovements() {
        return concurrentIntraBrokerPartitionMovements;
    }

    public void setConcurrentIntraBrokerPartitionMovements(int movements) {
        this.concurrentIntraBrokerPartitionMovements = movements;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("The upper bound of ongoing partition leadership movements. Default is 1000. " +
            "This configuration has been deprecated, use `spec.config` with key `concurrent_leader_movements` instead.")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getConcurrentLeaderMovements() {
        return concurrentLeaderMovements;
    }

    public void setConcurrentLeaderMovements(int movements) {
        this.concurrentLeaderMovements = movements;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("The upper bound, in bytes per second, on the bandwidth used to move replicas. There is no limit by default. " +
            "This configuration has been deprecated, use `spec.config` with key `replication_throttle` instead.")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getReplicationThrottle() {
        return replicationThrottle;
    }

    public void setReplicationThrottle(long bandwidth) {
        this.replicationThrottle = bandwidth;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.config", removalVersion = "v2")
    @Description("A list of strategy class names used to determine the execution order for the replica movements in the generated optimization proposal. " +
        "By default BaseReplicaMovementStrategy is used, which will execute the replica movements in the order that they were generated. " +
        "This configuration has been deprecated, use `spec.config` with key `replica_movement_strategies` instead.")
    public List<String> getReplicaMovementStrategies() {
        return replicaMovementStrategies;
    }

    public void setReplicaMovementStrategies(List<String> replicaMovementStrategies) {
        this.replicaMovementStrategies = replicaMovementStrategies;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = "spec.nodes", removalVersion = "v2")
    @Description("List of brokers and their corresponding volumes from which replicas need to be moved. " +
            "This configuration has been deprecated, use `spec.nodes` instead.")
    @MinimumItems(1)
    public List<RebalanceNode> getMoveReplicasOffVolumes() {
        return moveReplicasOffVolumes;
    }

    public void setMoveReplicasOffVolumes(List<RebalanceNode> moveReplicasOffVolumes) {
        this.moveReplicasOffVolumes = moveReplicasOffVolumes;
    }
}

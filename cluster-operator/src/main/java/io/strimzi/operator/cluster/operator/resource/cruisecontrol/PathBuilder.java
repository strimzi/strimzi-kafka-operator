/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIds;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds the path to the Cruise Control APi
 */
public class PathBuilder {

    String constructedPath;
    boolean firstParam;

    /**
     * Constructor
     *
     * @param endpoint  Cruise Control endpoint
     */
    public PathBuilder(CruiseControlEndpoints endpoint) {
        constructedPath = endpoint + "?";
        firstParam = true;
    }

    /**
     * Adds parameter with value to the path
     *
     * @param param     Cruise Control parameter
     * @param value     Parameter value
     *
     * @return  Instance of this builder
     */
    public PathBuilder withParameter(CruiseControlParameters param, String value) {
        if (!firstParam) {
            constructedPath += "&";
        } else {
            firstParam = false;
        }
        try {
            constructedPath += param.asPair(value);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

    /**
     * Adds parameter with multiple values to the path
     *
     * @param param     Cruise Control parameter
     * @param values    List of parameter value
     *
     * @return  Instance of this builder
     */
    public PathBuilder withParameter(CruiseControlParameters param, List<String> values) {
        if (!firstParam) {
            constructedPath += "&";
        } else {
            firstParam = false;
        }
        try {
            constructedPath += param.asList(values);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

    /**
     * Converts the list of broker and volume ID's to a list of string
     *
     * @param values    List of parameter value
     *
     * @return  Instance of this builder
     */
    private List<String> brokerAndVolumeIdsParameterList(List<BrokerAndVolumeIds> values) {

        List<String> brokerandVolumeIds = new ArrayList<>(values.size());
        for (BrokerAndVolumeIds brokerAndVolumeIds : values) {
            brokerAndVolumeIds.getVolumeIds().forEach(volumeId -> brokerandVolumeIds.add(brokerAndVolumeIds.getBrokerId() + "-/var/lib/kafka/data-" + volumeId + "/kafka-log" + brokerAndVolumeIds.getBrokerId()));
        }

        return brokerandVolumeIds;
    }

    private void addIfNotZero(PathBuilder builder, CruiseControlParameters param, long value) {
        if (value > 0) {
            builder.withParameter(param, String.valueOf(value));
        }
    }

    /**
     * Adds Rebalance parameters to the path
     *
     * @param options   Rebalance options
     *
     * @return  Instance of this builder
     */
    public PathBuilder withRebalanceParameters(RebalanceOptions options) {
        if (options != null) {
            PathBuilder builder = withAbstractRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.REBALANCE_DISK, String.valueOf(options.isRebalanceDisk()));
            addIfNotZero(builder, CruiseControlParameters.CONCURRENT_INTRA_PARTITION_MOVEMENTS, options.getConcurrentIntraBrokerPartitionMovements());
            return builder;
        } else {
            return this;
        }
    }

    private PathBuilder withAbstractRebalanceParameters(AbstractRebalanceOptions options) {
        if (options != null) {
            PathBuilder builder = withParameter(CruiseControlParameters.DRY_RUN, String.valueOf(options.isDryRun()))
                    .withParameter(CruiseControlParameters.VERBOSE, String.valueOf(options.isVerbose()))
                    .withParameter(CruiseControlParameters.SKIP_HARD_GOAL_CHECK, String.valueOf(options.isSkipHardGoalCheck()));

            if (options.getExcludedTopics() != null) {
                builder.withParameter(CruiseControlParameters.EXCLUDED_TOPICS, options.getExcludedTopics());
            }
            if (options.getReplicaMovementStrategies() != null) {
                builder.withParameter(CruiseControlParameters.REPLICA_MOVEMENT_STRATEGIES, options.getReplicaMovementStrategies());
            }

            addIfNotZero(builder, CruiseControlParameters.CONCURRENT_PARTITION_MOVEMENTS, options.getConcurrentPartitionMovementsPerBroker());
            addIfNotZero(builder, CruiseControlParameters.CONCURRENT_LEADER_MOVEMENTS, options.getConcurrentLeaderMovements());
            addIfNotZero(builder, CruiseControlParameters.REPLICATION_THROTTLE, options.getReplicationThrottle());

            if (options.getGoals() != null) {
                builder.withParameter(CruiseControlParameters.GOALS, options.getGoals());
            }
            return builder;
        } else {
            return this;
        }
    }

    /**
     * Adds add-broker options to the path
     *
     * @param options   Add broker options
     *
     * @return  Instance of this builder
     */
    public PathBuilder withAddBrokerParameters(AddBrokerOptions options) {
        if (options != null) {
            return withAbstractRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID, options.getBrokers().stream().map(String::valueOf).collect(Collectors.joining(",")));
        } else {
            return this;
        }
    }

    /**
     * Adds remove broker options to the path
     *
     * @param options   Remove-broker options
     *
     * @return  Instance of this builder
     */
    public PathBuilder withRemoveBrokerParameters(RemoveBrokerOptions options) {
        if (options != null) {
            return withAbstractRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID, options.getBrokers().stream().map(String::valueOf).collect(Collectors.joining(",")));
        } else {
            return this;
        }
    }

    /**
     * Adds remove disks options to the path
     *
     * @param options   Remove-disks options
     *
     * @return  Instance of this builder
     */
    public PathBuilder withRemoveBrokerDisksParameters(RemoveDisksOptions options) {
        if (options != null) {
            return withParameter(CruiseControlParameters.DRY_RUN, String.valueOf(options.isDryRun())).
                    withParameter(CruiseControlParameters.BROKER_ID_AND_LOG_DIRS, brokerAndVolumeIdsParameterList(options.getBrokersandVolumeIds()));
        } else {
            return this;
        }
    }

    /**
     * @return  Builds and returns the path
     */
    public String build() {
        return constructedPath;
    }

}

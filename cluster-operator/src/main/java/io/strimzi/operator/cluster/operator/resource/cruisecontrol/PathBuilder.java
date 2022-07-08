/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class PathBuilder {

    String constructedPath;
    boolean firstParam;

    public PathBuilder(CruiseControlEndpoints endpoint) {
        constructedPath = endpoint.path + "?";
        firstParam = true;
    }

    public PathBuilder withParameter(String parameter) {
        if (!firstParam) {
            constructedPath += "&";
        } else {
            firstParam = false;
        }
        try {
            constructedPath += URLEncoder.encode(parameter, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

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

    private void addIfNotZero(PathBuilder builder, CruiseControlParameters param, long value) {
        if (value > 0) {
            builder.withParameter(param, String.valueOf(value));
        }
    }

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

    public PathBuilder withAddBrokerParameters(AddBrokerOptions options) {
        if (options != null) {
            PathBuilder builder = withAbstractRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID, options.getBrokers().stream().map(String::valueOf).collect(Collectors.joining(",")));
            return builder;
        } else {
            return this;
        }
    }

    public PathBuilder withRemoveBrokerParameters(RemoveBrokerOptions options) {
        if (options != null) {
            PathBuilder builder = withAbstractRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID, options.getBrokers().stream().map(String::valueOf).collect(Collectors.joining(",")));
            return builder;
        } else {
            return this;
        }
    }

    public String build() {
        return constructedPath;
    }

}

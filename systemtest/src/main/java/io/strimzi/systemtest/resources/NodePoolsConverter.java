/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.systemtest.Environment;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class responsible for handling {@link io.strimzi.api.kafka.model.nodepool.KafkaNodePool} resources in test execution.
 * Based on {@link io.strimzi.systemtest.Environment#STRIMZI_FEATURE_GATES} and {@link io.strimzi.systemtest.Environment#STRIMZI_NODE_POOLS_ROLE_MODE}
 * how the NodePools should look like, if they should be applied both or just one.
 * For example, in case that:
 *  - we are using KafkaNodePools feature gate, but KRaft is disabled, we apply just Broker NodePool (just with broker role)
 *  - we are using KafkaNodePools feature gate, KRaft is enabled, then the Broker and Controller NodePools are applied with:
 *          - separate roles (so as a Broker and Controller) if {@link Environment#isSeparateRolesMode()} is true
 *          - mixed roles, the NodePool names are renamed (so they are not Brokers and Controllers), otherwise
 *  - we are not using KafkaNodePools feature gate, KRaft is disabled, we are not applying any of the NodePools
 *
 *  This handler is needed in STs to switch between modes without a problem - and also to make the whole process less confusing.
 */
public class NodePoolsConverter {

    public static KafkaNodePool[] convertNodePoolsIfNeeded(KafkaNodePool... nodePoolsToBeConverted) {
        List<KafkaNodePool> nodePools = Arrays.asList(nodePoolsToBeConverted);

        changeNodePoolsToHaveMixedRoles(nodePools);

        return removeNodePoolsFromArrayIfNeeded(nodePools).toArray(new KafkaNodePool[0]);
    }

    private static void changeNodePoolsToHaveMixedRoles(List<KafkaNodePool> nodePools) {
        if (!Environment.isSeparateRolesMode()) {
            nodePools
                .forEach(nodePool -> nodePool.getSpec().setRoles(List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)));
        }
    }

    private static List<KafkaNodePool> removeNodePoolsFromArrayIfNeeded(List<KafkaNodePool> nodePools) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return nodePools;
            }

            return nodePools
                .stream()
                .filter(kafkaNodePool -> kafkaNodePool.getSpec().getRoles().stream().noneMatch(role -> role.equals(ProcessRoles.CONTROLLER)))
                .collect(Collectors.toList());
        }

        return Collections.emptyList();
    }
}

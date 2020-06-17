/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaRebalanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRebalanceUtils.class);

    private KafkaRebalanceUtils() {}

    public enum KafkaRebalanceState {
        New,
        PendingProposal,
        ProposalReady,
        Rebalancing,
        Ready,
        NotReady,
        Stopped
    }

    private static Condition rebalanceStateCondition(String resourceName) {

        List<Condition> statusConditions = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(kubeClient().getNamespace())
                .withName(resourceName).get().getStatus().getConditions().stream()
                .filter(condition -> condition.getType() != null)
                .filter(condition -> Arrays.stream(KafkaRebalanceState.values()).anyMatch(stateValue -> stateValue.toString().equals(condition.getType())))
                .collect(Collectors.toList());

        if (statusConditions.size() == 1) {
            return statusConditions.get(0);
        } else if (statusConditions.size() > 1) {
            LOGGER.warn("Multiple KafkaRebalance State Conditions were present in the KafkaRebalance status");
            throw new RuntimeException("Multiple KafkaRebalance State Conditions were present in the KafkaRebalance status");
        } else {
            LOGGER.warn("No KafkaRebalance State Conditions were present in the KafkaRebalance status");
            throw new RuntimeException("No KafkaRebalance State Conditions were present in the KafkaRebalance status");
        }
    }

    // TODO: after revert of changes related to status we should you --> ResourceManager.waitForResourceStatus()
    public static void waitForKafkaRebalanceCustomResourceState(String resourceName, KafkaRebalanceState state) {
        LOGGER.info("Waiting for KafkaRebalance to be in the {} state", state);

        TestUtils.waitFor("KafkaRebalance to be in the " + state.name(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> rebalanceStateCondition(resourceName).getType().equals(state.toString()),
            () -> ResourceManager.logCurrentResourceStatus(KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(kubeClient().getNamespace())
                .withName(resourceName).get())
        );
    }

}

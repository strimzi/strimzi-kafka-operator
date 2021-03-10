/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaRebalanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRebalanceUtils.class);

    private KafkaRebalanceUtils() {}

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

    public static boolean waitForKafkaRebalanceCustomResourceState(String resourceName, KafkaRebalanceState state) {
        KafkaRebalance kafkaRebalance = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(kubeClient().getNamespace()).withName(resourceName).get();
        return ResourceManager.waitForResourceStatus(KafkaRebalanceResource.kafkaRebalanceClient(), kafkaRebalance, state, ResourceOperation.getTimeoutForKafkaRebalanceState(state));
    }

    public static String annotateKafkaRebalanceResource(String resourceName, KafkaRebalanceAnnotation annotation) {
        LOGGER.info("Annotating KafkaRebalance:{} with annotation {}", resourceName, annotation.toString());
        return ResourceManager.cmdKubeClient().namespace(kubeClient().getNamespace())
            .execInCurrentNamespace("annotate", "kafkarebalance", resourceName, Annotations.ANNO_STRIMZI_IO_REBALANCE + "=" + annotation.toString())
            .out()
            .trim();
    }

    public static void doRebalancingProcess(String rebalanceName) {
        // it can sometimes happen that KafkaRebalance is already in the ProposalReady state -> race condition prevention
        if (!rebalanceStateCondition(rebalanceName).getType().equals(KafkaRebalanceState.ProposalReady.name())) {
            LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceState.PendingProposal);

            waitForKafkaRebalanceCustomResourceState(rebalanceName, KafkaRebalanceState.PendingProposal);

            LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceState.ProposalReady);

            waitForKafkaRebalanceCustomResourceState(rebalanceName, KafkaRebalanceState.ProposalReady);
        }

        LOGGER.info("Triggering the rebalance with annotation {} of KafkaRebalance resource", "strimzi.io/rebalance=approve");

        String response = annotateKafkaRebalanceResource(rebalanceName, KafkaRebalanceAnnotation.approve);

        LOGGER.info("Response from the annotation process {}", response);

        LOGGER.info("Verifying that annotation triggers the {} state", KafkaRebalanceState.Rebalancing);

        waitForKafkaRebalanceCustomResourceState(rebalanceName, KafkaRebalanceState.Rebalancing);

        LOGGER.info("Verifying that KafkaRebalance is in the {} state", KafkaRebalanceState.Ready);

        waitForKafkaRebalanceCustomResourceState(rebalanceName, KafkaRebalanceState.Ready);
    }
}

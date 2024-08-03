/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaRebalanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRebalanceUtils.class);

    private KafkaRebalanceUtils() {}

    private static Condition rebalanceStateCondition(String namespace, String resourceName) {

        List<Condition> statusConditions = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace)
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

    public static boolean waitForKafkaRebalanceCustomResourceState(String namespace, String resourceName, KafkaRebalanceState state) {
        KafkaRebalance kafkaRebalance = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace).withName(resourceName).get();
        return ResourceManager.waitForResourceStatus(KafkaRebalanceResource.kafkaRebalanceClient(), kafkaRebalance, state, ResourceOperation.getTimeoutForKafkaRebalanceState(state));
    }

    public static boolean waitForKafkaRebalanceCustomResourceState(String resourceName, KafkaRebalanceState state) {
        return waitForKafkaRebalanceCustomResourceState(kubeClient().getNamespace(), resourceName, state);
    }

    public static String annotateKafkaRebalanceResource(String namespace, String resourceName, KafkaRebalanceAnnotation annotation) {
        LOGGER.info("Annotating KafkaRebalance: {} with annotation: {}", resourceName, annotation.toString());
        return ResourceManager.cmdKubeClient().namespace(namespace)
            .execInCurrentNamespace("annotate", "kafkarebalance", resourceName, Annotations.ANNO_STRIMZI_IO_REBALANCE + "=" + annotation.toString())
            .out()
            .trim();
    }

    public static String annotateKafkaRebalanceResource(String resourceName, KafkaRebalanceAnnotation annotation) {
        return annotateKafkaRebalanceResource(kubeClient().getNamespace(), resourceName, annotation);
    }

    public static void doRebalancingProcessWithAutoApproval(String namespace, String rebalanceName) {
        doRebalancingProcess(namespace, rebalanceName, true);
    }

    public static void doRebalancingProcess(String namespace, String rebalanceName) {
        doRebalancingProcess(namespace, rebalanceName, false);
    }

    public static void doRebalancingProcess(String namespace, String rebalanceName, boolean autoApproval) {
        LOGGER.info(String.join("", Collections.nCopies(76, "=")));
        LOGGER.info(KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace).withName(rebalanceName).get().getStatus().getConditions().get(0).getType());
        LOGGER.info(String.join("", Collections.nCopies(76, "=")));

        if (!autoApproval) {
            // it can sometimes happen that KafkaRebalance is already in the ProposalReady state -> race condition prevention
            if (!rebalanceStateCondition(namespace, rebalanceName).getType().equals(KafkaRebalanceState.ProposalReady.name())) {
                LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceState.ProposalReady);

                waitForKafkaRebalanceCustomResourceState(namespace, rebalanceName, KafkaRebalanceState.ProposalReady);
            }

            LOGGER.info(String.join("", Collections.nCopies(76, "=")));
            LOGGER.info(KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace).withName(rebalanceName).get().getStatus().getConditions().get(0).getType());
            LOGGER.info(String.join("", Collections.nCopies(76, "=")));

            // using automatic-approval annotation
            final KafkaRebalance kr = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace).withName(rebalanceName).get();
            if (kr.getMetadata().getAnnotations().containsKey(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL) &&
                kr.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL).equals("true")) {
                LOGGER.info("Triggering the rebalance automatically (because Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL is set to true) " +
                    "without an annotation {} of KafkaRebalance resource", "strimzi.io/rebalance=approve");
            } else {
                LOGGER.info("Triggering the rebalance with annotation {} of KafkaRebalance resource", "strimzi.io/rebalance=approve");

                String response = annotateKafkaRebalanceResource(namespace, rebalanceName, KafkaRebalanceAnnotation.approve);

                LOGGER.info("Response from the annotation process {}", response);
            }
        }

        LOGGER.info("Verifying that annotation triggers the {} state", KafkaRebalanceState.Rebalancing);

        waitForKafkaRebalanceCustomResourceState(namespace, rebalanceName, KafkaRebalanceState.Rebalancing);

        LOGGER.info("Verifying that KafkaRebalance is in the {} state", KafkaRebalanceState.Ready);

        waitForKafkaRebalanceCustomResourceState(namespace, rebalanceName, KafkaRebalanceState.Ready);
    }

    public static void waitForRebalanceStatusStability(String namespace, String resourceName) {
        int[] stableCounter = {0};

        KafkaRebalanceStatus oldStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace).withName(resourceName).get().getStatus();

        TestUtils.waitFor("KafkaRebalance status to be stable", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespace).withName(resourceName).get().getStatus().equals(oldStatus)) {
                stableCounter[0]++;
                if (stableCounter[0] == TestConstants.GLOBAL_STABILITY_OFFSET_COUNT) {
                    LOGGER.info("KafkaRebalance status is stable for: {} poll intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("KafkaRebalance status is not stable. Going to set the counter to zero");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("KafkaRebalance status gonna be stable in {} polls", TestConstants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
            return false;
        });
    }

    public static boolean waitForKafkaRebalanceReadiness(KafkaRebalance resource) {
        List<String> readyConditions = Arrays.asList(KafkaRebalanceState.PendingProposal.toString(), KafkaRebalanceState.ProposalReady.toString(), KafkaRebalanceState.Ready.toString());
        LOGGER.log(ResourceManager.getInstance().determineLogLevel(), "Waiting for {}: {}/{} will have desired ready state ({})", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), readyConditions);

        TestUtils.waitFor(String.format("%s: %s#%s will have desired state: %s", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), KafkaRebalanceState.Ready),
            TestConstants.GLOBAL_POLL_INTERVAL, ResourceOperation.getTimeoutForKafkaRebalanceState(KafkaRebalanceState.Ready),
            () -> {
                List<String> actualConditions = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).get().getStatus().getConditions().stream().map(Condition::getType).toList();

                for (String condition: actualConditions) {
                    if (readyConditions.contains(condition)) {
                        LOGGER.log(ResourceManager.getInstance().determineLogLevel(), "{}: {}/{} is in ready state ({})", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), condition);
                        return true;
                    }
                }
                LOGGER.log(ResourceManager.getInstance().determineLogLevel(), "{}: {}/{} is not ready ({})", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), actualConditions);

                return false;
            });

        return true;
    }
}

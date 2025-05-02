/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.New;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.NotReady;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.PendingProposal;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.ProposalReady;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.Ready;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.Rebalancing;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.COMPLETED_BYTE_MOVEMENT_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.ESTIMATED_TIME_TO_COMPLETION_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.EXECUTOR_STATE_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.REBALANCE_PROGRESS_CONFIG_MAP_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.ACTIVE;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED_WITH_ERROR;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.IN_EXECUTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorProgressTest extends AbstractKafkaRebalanceAssemblyOperatorTest  {

    private KafkaRebalanceStatus getKafkaRebalanceStatus() {
        KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).get();
        return kafkaRebalance.getStatus();
    }

    private record RebalanceConfigMapFields(
            boolean containsEstimatedTimeToCompletion,
            boolean containsCompletedByteMovement,
            boolean containsExecutorState,
            boolean containsBrokerLoadKey
    ) { }

    private static final Map<KafkaRebalanceState, Boolean> CONFIG_MAP_EXISTS_DURING_STATE = Map.of(
            New, false,
            PendingProposal, false,
            ProposalReady, true,
            Rebalancing, true,
            Ready, true,
            NotReady, true
    );

    private static final Map<KafkaRebalanceState, RebalanceConfigMapFields> STATE_TO_EXPECTED_CONFIG_MAP_FIELDS = Map.of(
            New, new RebalanceConfigMapFields(false, false, false, false),
            PendingProposal, new RebalanceConfigMapFields(false, false, false, false),
            ProposalReady, new RebalanceConfigMapFields(false, true, false, true),
            Rebalancing, new RebalanceConfigMapFields(true, true, true, true),
            Ready, new RebalanceConfigMapFields(true, true, false, true),
            NotReady, new RebalanceConfigMapFields(false, true, true, true)
    );

    private Future<Void> verifyKafkaRebalanceStateAndMap(VertxTestContext context, KafkaRebalanceState kafkaRebalanceState, ConfigMapOperator configMapOperator) {
        assertState(context, client, namespace, RESOURCE_NAME, kafkaRebalanceState);

        // Checks `KafkaRebalance` ConfigMap contains expected fields
        RebalanceConfigMapFields expectedFields = STATE_TO_EXPECTED_CONFIG_MAP_FIELDS.get(kafkaRebalanceState);
        return configMapOperator.getAsync(namespace, RESOURCE_NAME)
                .compose(configMap -> {
                    if (configMap != null) {
                        assertThat(CONFIG_MAP_EXISTS_DURING_STATE.get(kafkaRebalanceState), is(true));
                        assertThat(getKafkaRebalanceStatus().getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(true));

                        Map<String, String> fields = configMap.getData();
                        assertThat(fields.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(expectedFields.containsEstimatedTimeToCompletion));
                        assertThat(fields.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(expectedFields.containsCompletedByteMovement));
                        assertThat(fields.containsKey(EXECUTOR_STATE_KEY), is(expectedFields.containsExecutorState));
                        assertThat(fields.containsKey(BROKER_LOAD_KEY), is(expectedFields.containsBrokerLoadKey));
                    } else {
                        assertThat(CONFIG_MAP_EXISTS_DURING_STATE.get(kafkaRebalanceState), is(false));
                        assertThat(getKafkaRebalanceStatus().getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(false));
                    }
                    return Future.succeededFuture();
                });
    }

    private Future<Void> mockTaskAndReconcile(CruiseControlUserTaskStatus taskStatus, boolean stateEndpointFetchError, Reconciliation reconciliation) {
        cruiseControlServer.mockTask(taskStatus, stateEndpointFetchError);
        return krao.reconcile(reconciliation);
    }

    /**
     * Test progress fields of `KafkaRebalance` resource and ConfigMap during KafkaRebalance lifecycle.
     */
    @Test
    public void testProgressFieldsDuringRebalanceLifecycle(VertxTestContext context) {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        cruiseControlServer.mockTask(ACTIVE, false);
        krao.reconcile(reconciliation)
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, PendingProposal, configMapOperator))
                .compose(v -> mockTaskAndReconcile(COMPLETED, false, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, ProposalReady, configMapOperator))
                .compose(v -> mockTaskAndReconcile(IN_EXECUTION, false, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, Rebalancing, configMapOperator))
                .compose(v -> mockTaskAndReconcile(COMPLETED, false, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, Ready, configMapOperator))
                .onSuccess(v -> checkpoint.flag())
                .onFailure(context::failNow);
    }

    /**
     *  Test "Warning" is propagated to `KafkaRebalance` condition when Cruise Control REST API cannot be reached.
     */
    @Test
    public void testWarningConditionPropagationForUnreachableApi(VertxTestContext context) {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        cruiseControlServer.mockTask(COMPLETED, false);
        krao.reconcile(reconciliation)
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, ProposalReady, configMapOperator))
                .compose(v -> mockTaskAndReconcile(IN_EXECUTION, false, reconciliation))
                .compose(v -> mockTaskAndReconcile(IN_EXECUTION, true, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, Rebalancing, configMapOperator))
                .map(v -> {
                    // Test that warning condition was added to resource when Cruise Control API is unreachable.
                    Condition warningCondition1 = KafkaRebalanceUtils.getWarningCondition(getKafkaRebalanceStatus());
                    assertThat(warningCondition1, notNullValue());
                    return warningCondition1;
                })
                .compose(warningCondition1 -> mockTaskAndReconcile(IN_EXECUTION, true, reconciliation)
                        .map(v -> {
                            verifyKafkaRebalanceStateAndMap(context, Rebalancing, configMapOperator);

                            // Test that the warning condition was not updated.
                            Condition warningCondition2 = KafkaRebalanceUtils.getWarningCondition(getKafkaRebalanceStatus());
                            assertThat(warningCondition1.getReason(), is(warningCondition2.getReason()));
                            assertThat(warningCondition1.getMessage(), is(warningCondition2.getMessage()));
                            assertThat(warningCondition1.getLastTransitionTime(), is(warningCondition2.getLastTransitionTime()));

                            cruiseControlServer.mockTask(COMPLETED, false);
                            return null;
                        }))
                .compose(v ->  mockTaskAndReconcile(COMPLETED, true, reconciliation))
                .onSuccess(v -> {
                    verifyKafkaRebalanceStateAndMap(context, Ready, configMapOperator);

                    // Test that warning condition is removed
                    Condition warningCondition2 = KafkaRebalanceUtils.getWarningCondition(getKafkaRebalanceStatus());
                    assertThat(warningCondition2, nullValue());

                    checkpoint.flag();
                })
                .onFailure(context::failNow);
    }

    /**
     *  Test "Warning" is propagated to `KafkaRebalance` condition when executed task hasn't started.
     */
    @Test
    public void testWarningConditionPropagationForNonExecutingState(VertxTestContext context) {
        cruiseControlServer.mockTask(ACTIVE, false);

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(reconciliation)
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, PendingProposal, configMapOperator))
                .compose(v -> mockTaskAndReconcile(COMPLETED, true, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, ProposalReady, configMapOperator))
                .compose(v -> mockTaskAndReconcile(COMPLETED_WITH_ERROR, true, reconciliation))
                .onSuccess(v -> {
                    assertState(context, client, namespace, RESOURCE_NAME, Rebalancing);
                    Condition warningCondition = KafkaRebalanceUtils.getWarningCondition(getKafkaRebalanceStatus());
                    assertThat(warningCondition, notNullValue());
                    checkpoint.flag();
                })
                .onFailure(context::failNow);
    }

    /**
     *  Test progress fields of `KafkaRebalance` resource and ConfigMap after rebalance failure.
     */
    @Test
    public void testProgressFieldsOnRebalanceFailure(VertxTestContext context)  {
        cruiseControlServer.mockTask(COMPLETED, false);

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(reconciliation)
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, ProposalReady, configMapOperator))
                .compose(v -> mockTaskAndReconcile(IN_EXECUTION, false, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, Rebalancing, configMapOperator))
                .compose(v -> mockTaskAndReconcile(COMPLETED_WITH_ERROR, false, reconciliation))
                .compose(v -> verifyKafkaRebalanceStateAndMap(context, NotReady, configMapOperator))
                .onSuccess(v -> checkpoint.flag())
                .onFailure(context::failNow);
    }
}

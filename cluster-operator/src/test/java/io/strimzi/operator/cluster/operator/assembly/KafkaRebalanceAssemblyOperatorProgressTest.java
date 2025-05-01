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
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

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

    private void assertStatusHasProgressField(KafkaRebalanceStatus status) {
        assertThat(status.getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(Boolean.TRUE));
    }

    private void assertConfigMapHasProgressFields(ConfigMapOperator configMapOperator,
                                                  boolean containsEstimatedTimeToCompletion,
                                                  boolean containsCompletedByteMovement,
                                                  boolean containsExecutorState,
                                                  boolean containsBrokerLoadKey) {
        configMapOperator.getAsync(namespace, RESOURCE_NAME)
                .onSuccess(configMap -> {
                    Map<String, String> fields = configMap.getData();
                    assertThat(fields.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(containsEstimatedTimeToCompletion));
                    assertThat(fields.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(containsCompletedByteMovement));
                    assertThat(fields.containsKey(EXECUTOR_STATE_KEY), is(containsExecutorState));
                    assertThat(fields.containsKey(BROKER_LOAD_KEY), is(containsBrokerLoadKey));
                });
    }

    /**
     * Test progress fields of `KafkaRebalance` resource and ConfigMap during KafkaRebalance lifecycle.
     */
    @Test
    public void testProgressFieldsDuringRebalanceLifecycle(VertxTestContext context) {
        cruiseControlServer.mockTask(ACTIVE, false);

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from New to PendingProposal state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                    KafkaRebalanceStatus status = getKafkaRebalanceStatus();
                    assertThat(status.getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(Boolean.FALSE));
                    configMapOperator.getAsync(namespace, RESOURCE_NAME)
                            .onSuccess(configMap -> {
                                assertThat(configMap, nullValue());
                            });
                    cruiseControlServer.mockTask(COMPLETED, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName())))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from PendingProposal to ProposalReady state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, false, true, false, true);
                    cruiseControlServer.mockTask(IN_EXECUTION, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from ProposalReady to Rebalancing state.
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing);
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, true, true, true, true);
                    cruiseControlServer.mockTask(COMPLETED, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from Rebalancing to Ready state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Ready);
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, true, true, false, true);
                    checkpoint.flag();
                }));
    }

    /**
     *  Test "Warning" is propagated to `KafkaRebalance` condition when Cruise Control REST API cannot be reached.
     */
    @Test
    public void testWarningConditionPropagationForUnreachableApi(VertxTestContext context) {
        cruiseControlServer.mockTask(COMPLETED, false);

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from New to ProposalReady state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);

                    cruiseControlServer.mockTask(IN_EXECUTION, true);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .map(v -> {
                    // Check resource moved from ProposalReady to Rebalancing state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);
                    KafkaRebalanceStatus status = getKafkaRebalanceStatus();
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, true, true, true, true);

                    // Test that warning condition was added to resource when Cruise Control API is unreachable.
                    Condition warningCondition1 = KafkaRebalanceUtils.getWarningCondition(status);
                    assertThat(warningCondition1, notNullValue());
                    return warningCondition1;
                })
                .compose(warningCondition1 -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> {
                    // Check resource is still in Rebalancing state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);
                    KafkaRebalanceStatus status = getKafkaRebalanceStatus();
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, true, true, true, true);

                    // Test that the warning condition was not updated.
                    Condition warningCondition2 = KafkaRebalanceUtils.getWarningCondition(status);
                    assertThat(warningCondition1.getReason(), is(warningCondition2.getReason()));
                    assertThat(warningCondition1.getMessage(), is(warningCondition2.getMessage()));
                    assertThat(warningCondition1.getLastTransitionTime(), is(warningCondition2.getLastTransitionTime()));

                    cruiseControlServer.mockTask(COMPLETED, false);
                })))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource is in Ready state
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Ready);
                    KafkaRebalanceStatus status = getKafkaRebalanceStatus();
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, true, true, false, true);

                    // Test that warning condition is removed
                    Condition warningCondition2 = KafkaRebalanceUtils.getWarningCondition(status);
                    assertThat(warningCondition2, nullValue());

                    checkpoint.flag();
                }));
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

        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from New to PendingProposal state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                    cruiseControlServer.mockTask(COMPLETED, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from PendingProposal to ProposalReady state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    cruiseControlServer.mockTask(COMPLETED_WITH_ERROR, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from ProposalReady to Rebalancing state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);
                    KafkaRebalanceStatus status = getKafkaRebalanceStatus();
                    assertStatusHasProgressField(status);
                    assertConfigMapHasProgressFields(configMapOperator, false, false, false, true);

                    Condition warningCondition = KafkaRebalanceUtils.getWarningCondition(status);
                    assertThat(warningCondition, notNullValue());
                    checkpoint.flag();
                }));
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

        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from New to ProposalReady state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    cruiseControlServer.mockTask(IN_EXECUTION, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    cruiseControlServer.mockTask(COMPLETED_WITH_ERROR, false);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource moved from Rebalancing to NotReady state.
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.NotReady);
                    assertStatusHasProgressField(getKafkaRebalanceStatus());
                    assertConfigMapHasProgressFields(configMapOperator, false, true, true, true);
                    checkpoint.flag();
                }));
    }
}

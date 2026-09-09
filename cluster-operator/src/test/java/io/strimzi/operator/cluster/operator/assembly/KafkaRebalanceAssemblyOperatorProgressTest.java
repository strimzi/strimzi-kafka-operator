/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.New;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.NotReady;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.PendingProposal;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.ProposalReady;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.Ready;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.Rebalancing;
import static io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState.Stopped;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.EXECUTOR_STATE_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.ACTIVE;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.COMPLETED_WITH_ERROR;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus.IN_EXECUTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorProgressTest extends AbstractKafkaRebalanceAssemblyOperatorTest {

    @BeforeEach
    @Override
    public void beforeEach(TestInfo testInfo) {
        super.beforeEach(testInfo);

        // Create Kafka custom resource
        crdCreateKafka();

        // Create Cruise Control secrets
        crdCreateCruiseControlSecrets();

        // Create KafkaRebalance custom resource
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
    }

    private record RebalanceConfigMap(
            boolean configMapExpected,
            boolean containsEstimatedTimeToCompletion,
            boolean containsCompletedByteMovement,
            boolean containsExecutorState,
            boolean containsBrokerLoad) {
        void assertConfigMapFields(ConfigMap configMap) {
            if (configMapExpected) {
                assertThat(configMap, notNullValue());
                Map<String, String> fields = configMap.getData();
                assertThat(fields.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(containsEstimatedTimeToCompletion));
                assertThat(fields.containsKey(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is(containsCompletedByteMovement));
                assertThat(fields.containsKey(EXECUTOR_STATE_KEY), is(containsExecutorState));
                assertThat(fields.containsKey(BROKER_LOAD_KEY), is(containsBrokerLoad));
            } else {
                assertThat(configMap, nullValue());
            }
        }

        void assertConfigMapInStatus(KafkaRebalanceStatus status) {
            if (configMapExpected) {
                assertThat(status.getProgress(), notNullValue());
                assertThat(status.getProgress().getRebalanceProgressConfigMap(), not(emptyOrNullString()));
            } else {
                assertThat(status.getProgress(), nullValue());
            }
        }
    }

    private static final Map<KafkaRebalanceState, RebalanceConfigMap> STATE_TO_EXPECTED_CONFIG_MAP_FIELDS = Map.of(
            New, new RebalanceConfigMap(false, false, false, false, false),
            PendingProposal, new RebalanceConfigMap(false, false, false, false, false),
            ProposalReady, new RebalanceConfigMap(true, false, true, false, true),
            Rebalancing, new RebalanceConfigMap(true, true, true, true, true),
            Ready, new RebalanceConfigMap(true, true, true, false, true),
            NotReady, new RebalanceConfigMap(true, false, true, true, true)
    );

    private KafkaRebalanceStatus getKafkaRebalanceStatus() {
        return Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).get().getStatus();
    }

    private Future<Void> verifyKafkaRebalanceStateAndConfigMap(VertxTestContext context, KafkaRebalanceState state, ConfigMap configMap) {
        assertState(context, client, namespace, RESOURCE_NAME, state);

        RebalanceConfigMap expectations = STATE_TO_EXPECTED_CONFIG_MAP_FIELDS.get(state);
        expectations.assertConfigMapFields(configMap);
        expectations.assertConfigMapInStatus(getKafkaRebalanceStatus());

        return Future.succeededFuture();
    }

    private void verifyBrokerLoadRetained(VertxTestContext context, KafkaRebalanceState state, ConfigMap previous, ConfigMap current) {
        if (previous != null && current != null) {
            assertState(context, client, namespace, RESOURCE_NAME, state);
            assertThat(current.getData().get(BROKER_LOAD_KEY), is(previous.getData().get(BROKER_LOAD_KEY)));
        }
    }

    private void verifyBrokerLoadChanged(VertxTestContext context, KafkaRebalanceState state, ConfigMap previous, ConfigMap current) {
        assertState(context, client, namespace, RESOURCE_NAME, state);
        assertThat(current.getData().get(BROKER_LOAD_KEY), is(not(previous.getData().get(BROKER_LOAD_KEY))));
    }

    private Future<ConfigMap> reconcile(Reconciliation reconciliation) {
        return  krao.reconcile(reconciliation)
                .compose(res -> VertxUtil.toFuture(this.supplier.configMapOperations.getAsync(namespace, RESOURCE_NAME)));
    }

    private Future<Void> mockCruiseControlTask(CruiseControlUserTaskStatus taskStatus, boolean stateEndpointFetchError) {
        cruiseControlServer.mockTask(taskStatus, stateEndpointFetchError);
        return Future.succeededFuture();
    }

    private static Condition getWarningCondition(KafkaRebalanceStatus status) {
        return status.getConditions().stream()
                .filter(condition -> condition.getType().equals("Warning"))
                .findFirst()
                .orElse(null);
    }

    /**
     * Test progress fields of `KafkaRebalance` resource and ConfigMap during KafkaRebalance lifecycle.
     */
    @Test
    public void testProgressFieldsDuringRebalanceLifecycle(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(ACTIVE, false)
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, PendingProposal, res))

                .compose(res -> mockCruiseControlTask(COMPLETED, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, ProposalReady, res))

                .compose(res -> mockCruiseControlTask(IN_EXECUTION, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Rebalancing, res))

                .compose(res -> mockCruiseControlTask(COMPLETED, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Ready, res))

                .onSuccess(res -> checkpoint.flag())
                .onFailure(context::failNow);
    }

    /**
     *  Test "Warning" is propagated to `KafkaRebalance` condition when Cruise Control REST API cannot be reached.
     */
    @Test
    public void testWarningConditionPropagationForUnreachableApi(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(COMPLETED, false)
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, ProposalReady, res))

                .compose(res -> mockCruiseControlTask(IN_EXECUTION, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Rebalancing, res))

                .compose(res -> mockCruiseControlTask(IN_EXECUTION, true))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Rebalancing, res))
                .map(res -> {
                    // Test that warning condition was added to resource when Cruise Control API is unreachable.
                    Condition warningCondition1 = getWarningCondition(getKafkaRebalanceStatus());
                    assertThat(warningCondition1, notNullValue());
                    return warningCondition1;
                })
                .compose(warningCondition1 ->
                         mockCruiseControlTask(IN_EXECUTION, true)
                        .compose(res -> reconcile(reconciliation))
                        .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Rebalancing, res))
                        .onSuccess(res -> {

                            // Test that the warning condition was not updated.
                            Condition warningCondition2 = getWarningCondition(getKafkaRebalanceStatus());
                            assertThat(warningCondition1.getReason(), is(warningCondition2.getReason()));
                            assertThat(warningCondition1.getMessage(), is(warningCondition2.getMessage()));
                            assertThat(warningCondition1.getLastTransitionTime(), is(warningCondition2.getLastTransitionTime()));

                        }))
                .compose(res -> mockCruiseControlTask(COMPLETED, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Ready, res))
                .onSuccess(res -> {

                    // Test that warning condition is removed
                    Condition warningCondition2 = getWarningCondition(getKafkaRebalanceStatus());
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
        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(ACTIVE, false)
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, PendingProposal, res))

                .compose(res -> mockCruiseControlTask(COMPLETED, true))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, ProposalReady, res))

                .compose(res -> mockCruiseControlTask(COMPLETED_WITH_ERROR, true))
                .compose(res -> reconcile(reconciliation))
                .onSuccess(res -> {
                    assertState(context, client, namespace, RESOURCE_NAME, Rebalancing);
                    Condition warningCondition = getWarningCondition(getKafkaRebalanceStatus());
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
        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(COMPLETED, false)
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, ProposalReady, res))

                .compose(res -> mockCruiseControlTask(IN_EXECUTION, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, Rebalancing, res))

                .compose(res -> mockCruiseControlTask(COMPLETED_WITH_ERROR, false))
                .compose(res -> reconcile(reconciliation))
                .compose(res -> verifyKafkaRebalanceStateAndConfigMap(context, NotReady, res))

                .onSuccess(res -> checkpoint.flag())
                .onFailure(context::failNow);
    }

  /**
   * Tests that broker load data in the ConfigMap is retained across state transitions
   * and updated when a refresh annotation is applied.
   *
   * 1. A new KafkaRebalance resource is created with auto-approval; it is in the New state
   * 2. The resource moves through PendingProposal, ProposalReady, Rebalancing, and Ready states
   * 3. The broker load data is verified to be retained at each transition
   * 4. A refresh annotation is applied and the resource moves back to ProposalReady
   * 5. The broker load data is verified to have changed after refresh
   */
    @Test
    public void testConfigMapBrokerLoadDuringRebalanceLifecycle(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(ACTIVE, false)
                .compose(res -> reconcile(reconciliation))
                .onSuccess(cm1 -> verifyBrokerLoadRetained(context, PendingProposal, null, cm1))

                .compose(res -> mockCruiseControlTask(COMPLETED, false))
                .compose(res -> reconcile(reconciliation))
                .onSuccess(cm1 -> verifyBrokerLoadRetained(context, ProposalReady, null, cm1))

                .compose(cm1 -> mockCruiseControlTask(IN_EXECUTION, false)
                .compose(res -> reconcile(reconciliation))
                .onSuccess(cm2 -> verifyBrokerLoadRetained(context, Rebalancing, cm1, cm2))

                .compose(res -> mockCruiseControlTask(COMPLETED, false))
                .compose(res -> reconcile(reconciliation))
                .onSuccess(cm2 -> verifyBrokerLoadRetained(context, Ready, cm1, cm2))

                .compose(cm2 -> {

                    mockCruiseControlTask(COMPLETED, false);
                    cruiseControlServer.mockNewProposal();
                    annotate(client, namespace, RESOURCE_NAME, KafkaRebalanceAnnotation.refresh);

                    return reconcile(reconciliation)
                      .onSuccess(cm3 -> verifyBrokerLoadChanged(context, ProposalReady, cm2, cm3));
                }))
                .onSuccess(res -> checkpoint.flag())
                .onFailure(context::failNow);
    }

  /**
   * Tests that broker load data in the ConfigMap is updated when a
   * refresh annotation is applied in the 'ProposalReady' state.
   *
   * 1. A new KafkaRebalance resource is created with auto-approval; it is in the New state
   * 2. The resource moves to ProposalReady
   * 3. The Cruise Control mock is configured to return a new proposal with different broker load data
   * 4. A refresh annotation is applied and the resource stays in ProposalReady
   * 5. The broker load data is verified to have changed after refresh
   */
    @Test
    public void testConfigMapBrokerLoadFromProposalReadyAndRefresh(VertxTestContext context) {
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).edit(
            kr -> new KafkaRebalanceBuilder(kr)
                .editMetadata()
                    .removeFromAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL)
                .endMetadata()
                .build());

        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(COMPLETED, false)
              .compose(res -> reconcile(reconciliation))
              .onSuccess(cm1 -> verifyBrokerLoadRetained(context, ProposalReady, null, cm1))

                  .compose(cm1 -> {

                      mockCruiseControlTask(COMPLETED, false);
                      cruiseControlServer.mockNewProposal();
                      annotate(client, namespace, RESOURCE_NAME, KafkaRebalanceAnnotation.refresh);

                      return reconcile(reconciliation)
                          .onSuccess(cm2 -> verifyBrokerLoadChanged(context, ProposalReady, cm1, cm2));
                  })
              .onSuccess(res -> checkpoint.flag())
              .onFailure(context::failNow);
    }

   /**
    * Tests that broker load data in the ConfigMap is retained when
    * the resource transitions from 'ProposalReady' to 'Rebalancing' to 'Stopped'.
    *
    * 1. A new KafkaRebalance resource is created with auto-approval; it is in the New state
    * 2. The resource moves to ProposalReady
    * 3. The resource is auto-approved and moves to Rebalancing
    * 4. A stop annotation is applied and the resource moves to Stopped
    * 5. The broker load data is verified to be retained at each transition
    */
    @Test
    public void testBrokerLoadOnProposalReadyToRebalancingToStopped(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME);
        mockCruiseControlTask(COMPLETED, false)
              .compose(res -> reconcile(reconciliation))
              .onSuccess(cm1 -> verifyBrokerLoadRetained(context, ProposalReady, null, cm1))

                  .compose(cm1 -> mockCruiseControlTask(IN_EXECUTION, false)
                  .compose(res -> reconcile(reconciliation))
                  .onSuccess(cm2 -> verifyBrokerLoadRetained(context, Rebalancing, cm1, cm2))

                  .compose(cm2 -> {
                      cruiseControlServer.setupCCStopResponse();
                      annotate(client, namespace, RESOURCE_NAME, KafkaRebalanceAnnotation.stop);
                      return mockCruiseControlTask(COMPLETED, false)
                          .compose(res -> {
                              cruiseControlServer.setupCCStopResponse();
                              return reconcile(reconciliation);
                          })
                          .onSuccess(cm3 -> verifyBrokerLoadRetained(context, Stopped, cm1, cm3));
                  }))

              .onSuccess(res -> checkpoint.flag())
              .onFailure(context::failNow);
    }
}

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
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.BYTE_MOVEMENT_COMPLETED;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.BYTE_MOVEMENT_ZERO;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.COMPLETED_BYTE_MOVEMENT_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.ESTIMATED_TIME_TO_COMPLETION_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.EXECUTOR_STATE_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.REBALANCE_PROGRESS_CONFIG_MAP_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.TIME_COMPLETED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorProgressTest extends AbstractKafkaRebalanceAssemblyOperatorTest  {
    /**
     * Test progress fields of KafkaRebalance resource and ConfigMap during rebalance lifecycle.
     */
    @Test
    public void testProgressFieldsDuringRebalanceLifecycle(VertxTestContext context) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCRebalanceResponse(0, CruiseControlEndpoints.REBALANCE, "true");
        cruiseControlServer.setupCCStateResponseInExecution();
        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, 0);

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        ConfigMapOperator configMapOperator = this.supplier.configMapOperations;

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New to ProposalReady
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).get();
                    KafkaRebalanceStatus status = kafkaRebalance.getStatus();
                    assertThat(status.getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(Boolean.TRUE));

                    configMapOperator.getAsync(namespace, RESOURCE_NAME)
                            .onSuccess(configMap -> {
                                Map<String, String> fields = configMap.getData();
                                assertThat(fields.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(Boolean.FALSE));
                                assertThat(fields.get(COMPLETED_BYTE_MOVEMENT_KEY), is(BYTE_MOVEMENT_ZERO));
                                assertThat(fields.containsKey(EXECUTOR_STATE_KEY), is(Boolean.FALSE));
                                assertThat(fields.containsKey(BROKER_LOAD_KEY), is(Boolean.TRUE));
                            });
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing);
                    configMapOperator.getAsync(namespace, RESOURCE_NAME)
                            .onSuccess(configMap -> {
                                Map<String, String> fields = configMap.getData();
                                assertThat(fields.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(Boolean.TRUE));
                                assertThat(fields.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(Boolean.TRUE));
                                assertThat(fields.containsKey(EXECUTOR_STATE_KEY), is(Boolean.TRUE));
                                assertThat(fields.containsKey(BROKER_LOAD_KEY), is(Boolean.TRUE));
                            });
                })).compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Ready
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Ready);
                    configMapOperator.getAsync(namespace, RESOURCE_NAME)
                            .onSuccess(configMap -> {
                                Map<String, String> fields = configMap.getData();
                                assertThat(fields.get(ESTIMATED_TIME_TO_COMPLETION_KEY), is(TIME_COMPLETED));
                                assertThat(fields.get(COMPLETED_BYTE_MOVEMENT_KEY), is(BYTE_MOVEMENT_COMPLETED));
                                assertThat(fields.containsKey(EXECUTOR_STATE_KEY), is(Boolean.FALSE));
                                assertThat(fields.containsKey(BROKER_LOAD_KEY), is(Boolean.TRUE));
                            });
                    checkpoint.flag();
                }));
    }

    /**
     *  Test `KafkaRebalance` progress errors are correctly propagated to "Warning" conditions.
     */
    @Test
    public void testWarningConditionPropagation(VertxTestContext context) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCRebalanceResponse(0, CruiseControlEndpoints.REBALANCE, "true");
        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, 2);
        cruiseControlServer.setupCCStateNoResponse();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();
        
        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New to ProposalReady
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                }))
                .compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).get();
                    KafkaRebalanceStatus status = kafkaRebalance.getStatus();
                    assertThat(status.getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(Boolean.TRUE));

                    Condition warningCondition = KafkaRebalanceUtils.getWarningCondition(status);
                    assertThat(warningCondition, notNullValue());
                })).compose(v -> krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME)))
                .onComplete(context.succeeding(v -> {
                    // Check resource is still in Rebalancing state
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).get();
                    KafkaRebalanceStatus status = kafkaRebalance.getStatus();
                    assertThat(status.getProgress().containsKey(REBALANCE_PROGRESS_CONFIG_MAP_KEY), is(Boolean.TRUE));
                    checkpoint.flag();
                }));
    }

    // TODO: Add test for when `KafkaRebalance` is in New and Pending states
    // TODO: Add test for when CC provides malformed executor state  data
}

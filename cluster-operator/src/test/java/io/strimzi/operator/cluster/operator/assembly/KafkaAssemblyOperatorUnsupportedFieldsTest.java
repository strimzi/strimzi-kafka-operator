/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.text.ParseException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorUnsupportedFieldsTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    /**
     * This test checks that when the unsupported spec.topicOperator is not configured in the Kafka CR, no warning about
     * it will be in the status.
     */
    @Test
    public void testNoTopicOperatorWarnings(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                        .endListeners()
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kafkaCaptor.getValue(), is(notNullValue()));
                    assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
                    KafkaStatus status = kafkaCaptor.getValue().getStatus();

                    Condition ready = status.getConditions().stream()
                            .filter(condition -> "Ready".equals(condition.getType()))
                            .findFirst()
                            .orElse(null);
                    assertThat(ready, is(notNullValue()));
                    assertThat(ready.getStatus(), is("True"));

                    Condition toWarning = status.getConditions().stream()
                            .filter(condition -> "Warning".equals(condition.getType()) && "TopicOperator".equals(condition.getReason()))
                            .findFirst().orElse(null);
                    assertThat(toWarning, is(nullValue()));

                    async.flag();
                })));
    }

    /**
     * This test checks that when the unsupported spec.topicOperator is configured in the KAfka CR, a warning about it
     * will be in the status.
     */
    @Test
    public void testTopicOperatorWarnings(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                        .endListeners()
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kafkaCaptor.getValue(), is(notNullValue()));
                    assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
                    KafkaStatus status = kafkaCaptor.getValue().getStatus();

                    Condition ready = status.getConditions().stream()
                            .filter(condition -> "Ready".equals(condition.getType()))
                            .findFirst()
                            .orElse(null);
                    assertThat(ready, is(notNullValue()));
                    assertThat(ready.getStatus(), is("True"));

                    Condition toWarning = status.getConditions().stream()
                            .filter(condition -> "Warning".equals(condition.getType()) && "TopicOperator".equals(condition.getReason()))
                            .findFirst().orElse(null);
                    assertThat(toWarning, is(notNullValue()));
                    assertThat(toWarning.getMessage(), containsString("Kafka.spec.topicOperator is not supported anymore. Topic operator should be configured at path spec.entityOperator.topicOperator."));

                    async.flag();
                })));
    }

    class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.checkUnsupportedTopicOperator()
                    .map((Void) null);
        }

    }
}

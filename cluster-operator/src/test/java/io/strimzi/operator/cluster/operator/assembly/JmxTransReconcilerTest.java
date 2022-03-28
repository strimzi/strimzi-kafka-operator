/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.JmxTransSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.JmxTrans;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class JmxTransReconcilerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";

    private final JmxTransSpec jmxTransSpec = new JmxTransSpecBuilder()
            .withOutputDefinitions(new JmxTransOutputDefinitionTemplateBuilder()
                    .withName("standardOut")
                    .withOutputType("com.googlecode.jmxtrans.model.output.StdOutWriter")
                    .build())
            .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                    .withOutputs("standardOut")
                    .withAttributes("Count")
                    .withTargetMBean("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*")
                    .build())
            .build();

    @Test
    public void reconcileEnabledJmxTrans(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(JmxTransResources.serviceAccountName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(JmxTransResources.configMapName(NAME)), cmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(JmxTransResources.deploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(JmxTransResources.deploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(JmxTransResources.deploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30))
                .editSpec()
                    .withJmxTrans(jmxTransSpec)
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build()).build())
                    .endKafka()
                .endSpec()
                .build();

        JmxTransReconciler rcnclr = new JmxTransReconciler(Reconciliation.DUMMY_RECONCILIATION, 300_000, kafka, mockDepOps, mockCmOps, mockSaOps);

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(null, null)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(cmCaptor.getAllValues().size(), is(1));
                    assertThat(cmCaptor.getValue(), is(notNullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));
                    // Test that the configuration config map hash is set
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().getOrDefault(JmxTrans.ANNO_JMXTRANS_CONFIG_MAP_HASH, ""), is("0514fa2d"));

                    async.flag();
                })));
    }

    @Test
    public void reconcileDisabledJmxTrans(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(JmxTransResources.serviceAccountName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(JmxTransResources.configMapName(NAME)), cmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(JmxTransResources.deploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30);
        JmxTransReconciler rcnclr = new JmxTransReconciler(Reconciliation.DUMMY_RECONCILIATION, 300_000, kafka, mockDepOps, mockCmOps, mockSaOps);

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(null, null)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(cmCaptor.getAllValues().size(), is(1));
                    assertThat(cmCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }
}

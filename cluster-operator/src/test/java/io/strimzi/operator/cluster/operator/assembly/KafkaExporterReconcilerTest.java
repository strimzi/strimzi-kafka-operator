/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaExporterReconcilerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
    );

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled. In such case, the KE Deployment and all other
     * resources should be created ot updated. So the reconcile methods should be called with non-null values.
     */
    @Test
    public void reconcileWithEnabledExporter(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30))
                .editSpec()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().size(), is(1));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getPorts().size(), is(1));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(9404));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getFrom(), is(List.of()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled. In such case, the KE Deployment and all other
     * resources should be created ot updated. So the reconcile methods should be called with non-null values. However,
     * when the network policy generation is disabled, network policies should not be touched (so the reconcile should
     * not be called.)
     */
    @Test
    public void reconcileWithEnabledExporterWithoutNetworkPolicies(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30))
                .editSpec()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(notNullValue()));

                    verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is disabled. In such case, the KE Deployment and all other
     * resources should be deleted. So the reconcile methods should be called with null values.
     */
    @Test
    public void reconcileWithDisabledExporter(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30);

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is disabled. In such case, the KE Deployment and all other
     * resources should be deleted. So the reconcile methods should be called with null values. However, when the network
     * policy generation is disabled, network policies should not be touched (so the reconcile should not be called.)
     */
    @Test
    public void reconcileWithDisabledExporterWithoutNetworkPolicies(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30);

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));

                    verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }
}

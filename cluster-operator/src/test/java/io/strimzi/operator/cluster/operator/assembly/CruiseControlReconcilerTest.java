/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.balancing.BrokerCapacityBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CruiseControlReconcilerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private final CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
            .withBrokerCapacity(new BrokerCapacityBuilder().withInboundNetwork("10000KB/s").withOutboundNetwork("10000KB/s").build())
            .withConfig(Map.of("hard.goals", "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal"))
            .build();

    @Test
    public void reconcileEnabledCruiseControl(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceAccountName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceName(NAME)), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.networkPolicyName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.logAndMetricsConfigMapName(NAME)), cmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.deploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(CruiseControlResources.deploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(CruiseControlResources.deploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30))
                .editSpec()
                    .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();

        ClusterCa clusterCa = new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                new OpenSslCertManager(),
                new PasswordGenerator(10, "a", "a"),
                NAME,
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
        );

        CruiseControlReconciler rcnclr = new CruiseControlReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                VERSIONS,
                kafka.getSpec().getKafka().getStorage(),
                clusterCa
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(2));
                    assertThat(secretCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(secretCaptor.getAllValues().get(1), is(notNullValue()));

                    assertThat(serviceCaptor.getAllValues().size(), is(1));
                    assertThat(serviceCaptor.getValue(), is(notNullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));

                    assertThat(cmCaptor.getAllValues().size(), is(1));
                    assertThat(cmCaptor.getValue(), is(notNullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));

                    async.flag();
                })));
    }

    @Test
    public void reconcileDisabledCruiseControl(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceAccountName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceName(NAME)), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.networkPolicyName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.logAndMetricsConfigMapName(NAME)), cmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.deploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(CruiseControlResources.deploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(CruiseControlResources.deploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, "foo", 120, 30);

        ClusterCa clusterCa = new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                new OpenSslCertManager(),
                new PasswordGenerator(10, "a", "a"),
                NAME,
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
        );

        CruiseControlReconciler rcnclr = new CruiseControlReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                VERSIONS,
                kafka.getSpec().getKafka().getStorage(),
                clusterCa
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(2));
                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(secretCaptor.getAllValues().get(1), is(nullValue()));

                    assertThat(serviceCaptor.getAllValues().size(), is(1));
                    assertThat(serviceCaptor.getValue(), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));

                    assertThat(cmCaptor.getAllValues().size(), is(1));
                    assertThat(cmCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }
}

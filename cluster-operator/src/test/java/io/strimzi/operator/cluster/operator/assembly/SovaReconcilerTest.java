/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.sova.SovaResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.InternalCa;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertIssuer;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class SovaReconcilerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";

    private static final InternalCa CLUSTER_CA = new InternalCa(
            Reconciliation.DUMMY_RECONCILIATION,
            Ca.CaRole.CLUSTER_CA,
            new MockCertIssuer(),
            new PasswordGenerator(10, "a", "a"),
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, KafkaResources.clusterCaCertificateSecretName(NAME), MockCertIssuer.clusterCaCert(), MockCertIssuer.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, KafkaResources.clusterCaKeySecretName(NAME), MockCertIssuer.clusterCaKey()),
            new CaConfig(null, true)
    );

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(NAME)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(
                            new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                .endKafka()
            .endSpec()
            .build();

    /**
     * Tests Sova reconciliation when enabled — all resources should be created/updated.
     */
    @Test
    public void reconcileWithEnabledSova() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(SovaResources.secretName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(NAMESPACE), eq(SovaResources.configMapName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewExtraMetrics()
                        .withImage("my-image:latest")
                    .endExtraMetrics()
                .endSpec()
                .build();

        SovaReconciler reconciler = new SovaReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                CLUSTER_CA,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNotNull());

        ArgumentCaptor<Role> roleCaptor = ArgumentCaptor.forClass(Role.class);
        verify(mockRoleOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), roleCaptor.capture());
        assertThat(roleCaptor.getValue(), is(notNullValue()));
        assertThat(roleCaptor.getValue().getRules().get(0).getResourceNames(), containsInAnyOrder(
                KafkaResources.trustBundleSecretName(NAME),
                SovaResources.secretName(NAME)));

        ArgumentCaptor<RoleBinding> roleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        verify(mockRoleBindingOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.roleBindingName(NAME)), roleBindingCaptor.capture());
        assertThat(roleBindingCaptor.getValue(), is(notNullValue()));
        assertThat(roleBindingCaptor.getValue().getSubjects().get(0).getName(), is(SovaResources.componentName(NAME)));

        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.secretName(NAME)), isNotNull());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), netPolicyCaptor.capture());
        assertThat(netPolicyCaptor.getValue(), is(notNullValue()));
        // metrics port (9404) rule
        assertThat(netPolicyCaptor.getValue().getSpec().getIngress().size(), is(1));

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), depCaptor.capture());
        assertThat(depCaptor.getValue(), is(notNullValue()));
        assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
        assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
        assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(notNullValue()));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), pdbCaptor.capture());
        assertThat(pdbCaptor.getValue(), is(notNullValue()));
        assertThat(pdbCaptor.getValue().getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    /**
     * Tests Sova reconciliation when disabled — all resources should be deleted (reconciled with null).
     */
    @Test
    public void reconcileWithDisabledSova() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        // KAFKA has no extraMetrics spec — disabled
        Ca clusterCa = spy(CLUSTER_CA);
        SovaReconciler reconciler = new SovaReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                KAFKA,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                clusterCa,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNull());
        verify(mockRoleOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNull());
        verify(mockRoleBindingOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.roleBindingName(NAME)), isNull());
        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.secretName(NAME)), isNull());
        verify(clusterCa, times(1)).cleanupEndEntityCert(SovaResources.secretName(NAME));
        verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNull());
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNull());
        verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNull());
    }

    /**
     * Tests that when network policy generation is disabled, the network policy is never reconciled.
     */
    @Test
    public void reconcileWithEnabledSovaWithoutNetworkPolicies() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(SovaResources.secretName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(NAMESPACE), eq(SovaResources.configMapName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewExtraMetrics()
                        .withImage("my-image:latest")
                    .endExtraMetrics()
                .endSpec()
                .build();

        SovaReconciler reconciler = new SovaReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
                supplier,
                kafka,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                CLUSTER_CA,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNotNull());
        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.secretName(NAME)), isNotNull());
        verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNotNull());
        verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(SovaResources.componentName(NAME)), isNotNull());
    }
}

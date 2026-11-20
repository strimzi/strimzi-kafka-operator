/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.extrametrics.ExtraMetricsResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CertSecretUtils;
import io.strimzi.operator.cluster.model.ExtraMetrics;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
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
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
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
public class ExtraMetricsReconcilerTest {
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
     * Tests Extra Metrics reconciliation when enabled — all resources should be created/updated.
     */
    @Test
    public void reconcileWithEnabledExtraMetrics() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewExtraMetrics()
                        .withImage("my-image:latest")
                    .endExtraMetrics()
                .endSpec()
                .build();

        ExtraMetricsReconciler reconciler = new ExtraMetricsReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                CLUSTER_CA,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNotNull());

        ArgumentCaptor<Role> roleCaptor = ArgumentCaptor.forClass(Role.class);
        verify(mockRoleOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), roleCaptor.capture());
        assertThat(roleCaptor.getValue(), is(notNullValue()));
        assertThat(roleCaptor.getValue().getRules().get(0).getResourceNames(), containsInAnyOrder(
                KafkaResources.trustBundleSecretName(NAME),
                ExtraMetricsResources.secretName(NAME)));

        ArgumentCaptor<RoleBinding> roleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        verify(mockRoleBindingOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), roleBindingCaptor.capture());
        assertThat(roleBindingCaptor.getValue(), is(notNullValue()));
        assertThat(roleBindingCaptor.getValue().getSubjects().get(0).getName(), is(ExtraMetricsResources.componentName(NAME)));

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        verify(mockCmOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), cmCaptor.capture());
        assertThat(cmCaptor.getValue(), is(notNullValue()));
        assertThat(cmCaptor.getValue().getMetadata().getName(), is(ExtraMetricsResources.configMapName(NAME)));
        assertThat(cmCaptor.getValue().getData(), hasKey(LoggingModel.LOG4J2_CONFIG_MAP_KEY));

        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), secretCaptor.capture());
        Secret certSecret = secretCaptor.getValue();
        assertThat(certSecret, is(notNullValue()));
        assertThat(certSecret.getData(), hasKey(Ca.SecretEntry.CRT.asKey(ExtraMetrics.COMPONENT_TYPE)));
        assertThat(certSecret.getData(), hasKey(Ca.SecretEntry.KEY.asKey(ExtraMetrics.COMPONENT_TYPE)));

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), netPolicyCaptor.capture());
        assertThat(netPolicyCaptor.getValue(), is(notNullValue()));
        // metrics port (9404) rule
        assertThat(netPolicyCaptor.getValue().getSpec().getIngress().size(), is(1));

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), depCaptor.capture());
        assertThat(depCaptor.getValue(), is(notNullValue()));
        assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
        assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
        String certHash = depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
        assertThat(certHash, is(not(emptyOrNullString())));
        assertThat(certHash, is(CertSecretUtils.getCertificateShortThumbprint(certSecret, Ca.SecretEntry.CRT.asKey(ExtraMetrics.COMPONENT_TYPE))));
        verify(mockDepOps, times(1)).waitForObserved(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong());
        verify(mockDepOps, times(1)).readiness(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), pdbCaptor.capture());
        assertThat(pdbCaptor.getValue(), is(notNullValue()));
        assertThat(pdbCaptor.getValue().getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    /**
     * Tests Extra Metrics reconciliation when disabled — all resources should be deleted (reconciled with null).
     */
    @Test
    public void reconcileWithDisabledExtraMetrics() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        // KAFKA has no extraMetrics spec — disabled
        Ca clusterCa = spy(CLUSTER_CA);
        ExtraMetricsReconciler reconciler = new ExtraMetricsReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                KAFKA,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                clusterCa,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
        verify(mockRoleOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
        verify(mockRoleBindingOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), isNull());
        verify(mockCmOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), isNull());
        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), isNull());
        verify(clusterCa, times(1)).cleanupEndEntityCert(ExtraMetricsResources.secretName(NAME));
        verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
        verify(mockDepOps, never()).readiness(any(), any(), any(), anyLong(), anyLong());
        verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
    }

    /**
     * Tests Extra Metrics reconciliation when disabled and network policy and PodDisruptionBudget generation is disabled.
     * The other resources should be deleted, but network policies and PodDisruptionBudgets should not be touched.
     */
    @Test
    public void reconcileWithDisabledExtraMetricsWithoutNetworkPoliciesAndPdbs() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        Ca clusterCa = spy(CLUSTER_CA);
        ExtraMetricsReconciler reconciler = new ExtraMetricsReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false")
                        .with(ClusterOperatorConfig.POD_DISRUPTION_BUDGET_GENERATION.key(), "false")
                        .build(),
                supplier,
                KAFKA,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                clusterCa,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
        verify(mockRoleOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
        verify(mockRoleBindingOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), isNull());
        verify(mockCmOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), isNull());
        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), isNull());
        verify(clusterCa, times(1)).cleanupEndEntityCert(ExtraMetricsResources.secretName(NAME));
        verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());
        verify(mockPodDisruptionBudgetOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNull());
    }

    /**
     * Tests that when network policy generation is disabled, the network policy is never reconciled.
     */
    @Test
    public void reconcileWithEnabledExtraMetricsWithoutNetworkPolicies() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleOperator mockRoleOps = supplier.roleOperations;
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.roleBindingName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.configMapName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), any())).thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewExtraMetrics()
                        .withImage("my-image:latest")
                    .endExtraMetrics()
                .endSpec()
                .build();

        ExtraMetricsReconciler reconciler = new ExtraMetricsReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
                supplier,
                kafka,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                CLUSTER_CA,
                KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        reconciler.reconcile(false, null, null, Clock.systemUTC()).toCompletableFuture().join();

        verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNotNull());
        verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.secretName(NAME)), isNotNull());
        verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());
        verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNotNull());
        verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(ExtraMetricsResources.componentName(NAME)), isNotNull());
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneEncryptionConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.InternalCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertIssuer;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.platform.KubernetesVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class KafkaReconcilerServiceAccountTest {
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER_NAME = "my-cluster";
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();
    private final static InternalCa CLUSTER_CA = new InternalCa(
            Reconciliation.DUMMY_RECONCILIATION,
            Ca.CaRole.CLUSTER_CA,
            new OpenSslCertIssuer(),
            new PasswordGenerator(10, "a", "a"),
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME), MockCertIssuer.clusterCaCert(), MockCertIssuer.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, KafkaResources.clusterCaKeySecretName(CLUSTER_NAME), MockCertIssuer.clusterCaKey()),
            new CaConfig(null, true)
    );
    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .build())
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool KAFKA_NODE_POOL = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withDeleteClaim(true).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();

    private final static Reconciliation RECONCILIATION = new Reconciliation("test", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

    @Test
    public void testClusterOperatorServiceAccountIsCreatedWithServiceAccountAuthentication() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(),
                AuthenticationConfiguration.fromCrd(NAMESPACE, CLUSTER_NAME, new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build(), null));

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(CompletableFuture.completedFuture(ReconcileResult.created(new ServiceAccount())));

        MockKafkaReconciler reconciler = new MockKafkaReconciler(supplier, securityContext);
        reconciler.clusterOperatorServiceAccount().toCompletableFuture().join();
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        verify(mockSaOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorServiceAccount(CLUSTER_NAME)), saCaptor.capture());

        ServiceAccount sa = saCaptor.getValue();
        assertThat(sa, is(notNullValue()));
        assertThat(sa.getMetadata().getName(), is(KafkaResources.clusterOperatorServiceAccount(CLUSTER_NAME)));
        assertThat(sa.getMetadata().getNamespace(), is(NAMESPACE));
    }

    @Test
    public void testClusterOperatorServiceAccountIsDeletedWithoutServiceAccountAuthentication() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(CompletableFuture.completedFuture(ReconcileResult.deleted()));

        MockKafkaReconciler reconciler = new MockKafkaReconciler(supplier, securityContext);

        reconciler.clusterOperatorServiceAccount().toCompletableFuture().join();
        // The desired Service Account is null => the operator deletes it if it exists
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        verify(mockSaOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorServiceAccount(CLUSTER_NAME)), saCaptor.capture());
        assertThat(saCaptor.getValue(), is(nullValue()));
    }

    static class MockKafkaReconciler extends KafkaReconciler {
        MockKafkaReconciler(ResourceOperatorSupplier supplier, KafkaClusterSecurityContext securityContext) {
            super(RECONCILIATION, KAFKA, List.of(KAFKA_NODE_POOL), createKafkaCluster(supplier, securityContext), CLUSTER_CA, CLUSTER_CA, CO_CONFIG, supplier, PFA, Set.of());
        }

        private static KafkaCluster createKafkaCluster(ResourceOperatorSupplier supplier, KafkaClusterSecurityContext securityContext)   {
            return KafkaClusterCreator.createKafkaCluster(
                    RECONCILIATION,
                    KAFKA,
                    List.of(KAFKA_NODE_POOL),
                    Map.of(),
                    KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                    VERSIONS,
                    supplier.sharedEnvironmentProvider,
                    securityContext);
        }
    }
}

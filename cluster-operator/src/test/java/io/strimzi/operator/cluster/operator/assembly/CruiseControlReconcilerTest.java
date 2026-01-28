/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsersBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.operator.MockCertManager;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CruiseControlReconcilerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
    private static final Storage STORAGE = new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build();
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static Set<NodeRef> NODES = Set.of(
            new NodeRef(NAME + "-mixed-0", 0, "mixed", true, true),
            new NodeRef(NAME + "-mixed-1", 1, "mixed", true, true),
            new NodeRef(NAME + "-mixed-2", 2, "mixed", true, true));
    private static final String USER_MANAGED_API_SECRET_NAME = "cc-api-user-secret";
    private static final String USER_MANAGED_API_SECRET_KEY = "key";

    /**
     * This parameterized test uses '@CsvSource' to provide combinations of boolean values for the
     * topicOperatorEnabled and apiUsersEnabled parameters.
     * The provided combinations are:
     *   (a) true, true
     *   (b) true, false
     *   (c) false, true
     *   (d) false, false
     * This test verifies the behavior of the Cruise Control reconciler based on these different combinations of
     * parameter values.
     */
    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void reconcileEnabledCruiseControl(boolean topicOperatorEnabled, boolean apiUsersEnabled, VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PasswordGenerator mockPasswordGenerator = new PasswordGenerator(10, "a", "a");
        PodDisruptionBudgetOperator mockPodDisruptionBudget = supplier.podDisruptionBudgetOperator;

        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceAccountName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.secretName(NAME)), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(NAME)), any())).thenReturn(Future.succeededFuture());

        if (apiUsersEnabled) {
            Secret userManagedApiSecret = mock(Secret.class);
            doReturn(Map.of(USER_MANAGED_API_SECRET_KEY, Util.encodeToBase64("username0: password0,USER\nusername1: password1,VIEWER"))).when(userManagedApiSecret).getData();
            when(mockSecretOps.getAsync(eq(NAMESPACE), eq(USER_MANAGED_API_SECRET_NAME))).thenReturn(Future.succeededFuture(userManagedApiSecret));
        }

        if (topicOperatorEnabled) {
            Secret topicOperatorApiSecret = mock(Secret.class);
            doReturn(Map.of(TOPIC_OPERATOR_USERNAME_KEY, Util.encodeToBase64(TOPIC_OPERATOR_USERNAME), TOPIC_OPERATOR_PASSWORD_KEY, Util.encodeToBase64("changeit"))).when(topicOperatorApiSecret).getData();
            when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorCcApiSecretName(NAME)))).thenReturn(Future.succeededFuture(topicOperatorApiSecret));
        }

        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.networkPolicyName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.configMapName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockPodDisruptionBudget.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withBrokerCapacity(new BrokerCapacityBuilder().withInboundNetwork("10000KB/s").withOutboundNetwork("10000KB/s").build())
                        .withConfig(
                                Map.of("hard.goals", "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal",
                                        CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue(), "3")
                        )
                    .endCruiseControl()
                .endSpec()
                .build();

        if (apiUsersEnabled) {
            kafka.getSpec().getCruiseControl().setApiUsers(
                new HashLoginServiceApiUsersBuilder()
                    .withNewValueFrom()
                        .withSecretKeyRef(
                            new SecretKeySelectorBuilder()
                                .withKey(USER_MANAGED_API_SECRET_KEY)
                                .withName(USER_MANAGED_API_SECRET_NAME)
                            .build()
                        )
                    .endValueFrom()
                    .build()
            );
        }

        if (topicOperatorEnabled) {
            kafka.getSpec().setEntityOperator(
                new EntityOperatorSpecBuilder()
                    .withNewTopicOperator()
                    .endTopicOperator()
                    .build());
        }

        ClusterCa clusterCa = new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                new MockCertManager(),
                new PasswordGenerator(10, "a", "a"),
                NAME,
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
        );

        CruiseControlReconciler rcnclr = new CruiseControlReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                mockPasswordGenerator,
                kafka,
                VERSIONS,
                NODES,
                Map.of("mixed", STORAGE),
                Map.of(),
                clusterCa
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceAccountName(NAME)), isNotNull());

                    verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.secretName(NAME)), isNotNull());
                    verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(NAME)), isNotNull());

                    verify(mockServiceOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceName(NAME)), isNotNull());

                    verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.networkPolicyName(NAME)), isNotNull());

                    verify(mockCmOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.configMapName(NAME)), isNotNull());

                    // Verify deployment
                    ArgumentCaptor<Deployment> deployCaptor = ArgumentCaptor.forClass(Deployment.class);
                    verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), deployCaptor.capture());
                    assertThat(deployCaptor.getValue(), is(notNullValue()));
                    assertThat(deployCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
                    assertThat(deployCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
                    assertThat(deployCaptor.getAllValues().get(0).getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH), is("e104ddb6"));
                    assertThat(deployCaptor.getAllValues().get(0).getSpec().getTemplate().getMetadata().getAnnotations().get(CruiseControl.ANNO_STRIMZI_CAPACITY_CONFIGURATION_HASH), is("3a5e63e7"));
                    assertThat(deployCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is("4d715cdd"));
                    if (topicOperatorEnabled && apiUsersEnabled) {
                        assertThat(deployCaptor.getAllValues().get(0).getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_AUTH_HASH), is("8c2972b2"));
                    } else if (topicOperatorEnabled) {
                        assertThat(deployCaptor.getAllValues().get(0).getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_AUTH_HASH), is("1b601e9a"));
                    } else if (apiUsersEnabled) {
                        assertThat(deployCaptor.getAllValues().get(0).getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_AUTH_HASH), is("4215f758"));
                    } else {
                        assertThat(deployCaptor.getAllValues().get(0).getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_AUTH_HASH), is("5a188d9a"));
                    }

                    ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
                    verify(mockPodDisruptionBudget, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), pdbCaptor.capture());
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getMetadata().getName(), is(CruiseControlResources.componentName(NAME)));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

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
        PodDisruptionBudgetOperator mockPodDisruptionBudget = supplier.podDisruptionBudgetOperator;

        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceAccountName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.secretName(NAME)), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.networkPolicyName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.configMapName(NAME)), any())).thenReturn(Future.succeededFuture());

        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockPodDisruptionBudget.reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        ClusterCa clusterCa = new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                new MockCertManager(),
                new PasswordGenerator(10, "a", "a"),
                NAME,
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
        );

        CruiseControlReconciler rcnclr = new CruiseControlReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                new PasswordGenerator(16),
                KAFKA,
                VERSIONS,
                NODES,
                Map.of("mixed", STORAGE),
                Map.of(),
                clusterCa
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceAccountName(NAME)), isNull());

                    verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.secretName(NAME)), isNull());
                    verify(mockSecretOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(NAME)), isNull());

                    verify(mockServiceOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.serviceName(NAME)), isNull());

                    verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.networkPolicyName(NAME)), isNull());

                    verify(mockCmOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.configMapName(NAME)), isNull());

                    verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), isNull());

                    verify(mockPodDisruptionBudget, times(1)).reconcile(any(), eq(NAMESPACE), eq(CruiseControlResources.componentName(NAME)), isNull());

                    async.flag();
                })));
    }
}

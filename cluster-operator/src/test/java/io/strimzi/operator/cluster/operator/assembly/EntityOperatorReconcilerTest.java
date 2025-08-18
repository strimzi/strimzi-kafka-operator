/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.time.Clock;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class EntityOperatorReconcilerTest {
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

    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new MockCertManager(),
            new PasswordGenerator(10, "a", "a"),
            NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
    );

    @Test
    public void reconcileWithToAndUo(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        RoleOperator mockRoleOps = supplier.roleOperations;
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;


        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)))).thenReturn(Future.succeededFuture());
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> toSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> uoSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)), uoSecretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Role> operatorRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), operatorRoleCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<RoleBinding> toRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<RoleBinding> uoRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> toCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorLoggingConfigMapName(NAME)), toCmCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<ConfigMap> uoCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorLoggingConfigMapName(NAME)), uoCmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperatorReconciler rcnclr = new EntityOperatorReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(toSecretCaptor.getAllValues().size(), is(1));
                    assertThat(toSecretCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(uoSecretCaptor.getAllValues().size(), is(1));
                    assertThat(uoSecretCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));

                    assertThat(operatorRoleCaptor.getAllValues().size(), is(1));
                    assertThat(operatorRoleCaptor.getValue(), is(notNullValue()));

                    assertThat(toRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(toCmCaptor.getAllValues().size(), is(1));
                    assertThat(toCmCaptor.getValue(), is(notNullValue()));
                    assertThat(uoCmCaptor.getAllValues().size(), is(1));
                    assertThat(uoCmCaptor.getValue(), is(notNullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is("4d715cdd4d715cdd"));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    @Test
    public void reconcileWithToAndUoAndWatchNamespaces(VertxTestContext context) {
        String toWatchNamespace = "to-watch-namespace";
        String uoWatchNamespace = "uo-watch-namespace";

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        RoleOperator mockRoleOps = supplier.roleOperations;
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)))).thenReturn(Future.succeededFuture());
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> toSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> uoSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)), uoSecretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Role> operatorRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), operatorRoleCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Role> toRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(toWatchNamespace), eq(KafkaResources.entityOperatorDeploymentName(NAME)), toRoleCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Role> uoRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(uoWatchNamespace), eq(KafkaResources.entityOperatorDeploymentName(NAME)), uoRoleCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<RoleBinding> toRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockRoleBindingOps.reconcile(any(), eq(toWatchNamespace), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<RoleBinding> uoRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockRoleBindingOps.reconcile(any(), eq(uoWatchNamespace), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> toCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorLoggingConfigMapName(NAME)), toCmCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<ConfigMap> uoCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorLoggingConfigMapName(NAME)), uoCmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                            .withWatchedNamespace(toWatchNamespace)
                        .endTopicOperator()
                        .withNewUserOperator()
                            .withWatchedNamespace(uoWatchNamespace)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperatorReconciler rcnclr = new EntityOperatorReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(toSecretCaptor.getAllValues().size(), is(1));
                    assertThat(toSecretCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(uoSecretCaptor.getAllValues().size(), is(1));
                    assertThat(uoSecretCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));

                    assertThat(operatorRoleCaptor.getAllValues().size(), is(1));
                    assertThat(operatorRoleCaptor.getValue(), is(notNullValue()));
                    assertThat(toRoleCaptor.getAllValues().size(), is(1));
                    assertThat(toRoleCaptor.getValue(), is(notNullValue()));
                    assertThat(uoRoleCaptor.getAllValues().size(), is(1));
                    assertThat(uoRoleCaptor.getValue(), is(notNullValue()));

                    assertThat(toRoleBindingCaptor.getAllValues().size(), is(2));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0).getMetadata().getNamespace(), is(toWatchNamespace));
                    assertThat(toRoleBindingCaptor.getAllValues().get(1), is(notNullValue()));
                    assertThat(toRoleBindingCaptor.getAllValues().get(1).getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(uoRoleBindingCaptor.getAllValues().size(), is(2));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0).getMetadata().getNamespace(), is(uoWatchNamespace));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(1), is(notNullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(1).getMetadata().getNamespace(), is(NAMESPACE));

                    assertThat(toCmCaptor.getAllValues().size(), is(1));
                    assertThat(toCmCaptor.getValue(), is(notNullValue()));
                    assertThat(uoCmCaptor.getAllValues().size(), is(1));
                    assertThat(uoCmCaptor.getValue(), is(notNullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void reconcileWithToOnly(boolean cruiseControlEnabled, VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        RoleOperator mockRoleOps = supplier.roleOperations;
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> toSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        if (cruiseControlEnabled) {
            when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorCcApiSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        }
        ArgumentCaptor<Secret> uoSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)), uoSecretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Role> operatorRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), operatorRoleCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<RoleBinding> toRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<RoleBinding> uoRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> toCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorLoggingConfigMapName(NAME)), toCmCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<ConfigMap> uoCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorLoggingConfigMapName(NAME)), uoCmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        if (cruiseControlEnabled) {
            kafka.getSpec().setCruiseControl(new CruiseControlSpecBuilder().build());
        }

        EntityOperatorReconciler rcnclr = new EntityOperatorReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    if (cruiseControlEnabled) {
                        assertThat(toSecretCaptor.getAllValues().size(), is(2));
                        assertThat(toSecretCaptor.getAllValues().get(0), is(notNullValue()));
                        assertThat(toSecretCaptor.getAllValues().get(1), is(notNullValue()));
                    } else {
                        assertThat(toSecretCaptor.getAllValues().size(), is(1));
                        assertThat(toSecretCaptor.getAllValues().get(0), is(notNullValue()));
                    }
                    assertThat(uoSecretCaptor.getAllValues().size(), is(1));
                    assertThat(uoSecretCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));

                    assertThat(operatorRoleCaptor.getAllValues().size(), is(1));
                    assertThat(operatorRoleCaptor.getValue(), is(notNullValue()));

                    assertThat(toRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0), is(notNullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(toCmCaptor.getAllValues().size(), is(1));
                    assertThat(toCmCaptor.getValue(), is(notNullValue()));
                    assertThat(uoCmCaptor.getAllValues().size(), is(1));
                    assertThat(uoCmCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is("4d715cdd"));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    @Test
    public void reconcileWithUoOnly(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        RoleOperator mockRoleOps = supplier.roleOperations;
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> toSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> uoSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)), uoSecretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Role> operatorRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), operatorRoleCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<RoleBinding> toRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<RoleBinding> uoRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> toCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorLoggingConfigMapName(NAME)), toCmCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<ConfigMap> uoCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorLoggingConfigMapName(NAME)), uoCmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperatorReconciler rcnclr = new EntityOperatorReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(toSecretCaptor.getAllValues().size(), is(1));
                    assertThat(toSecretCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(uoSecretCaptor.getAllValues().size(), is(1));
                    assertThat(uoSecretCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));

                    assertThat(operatorRoleCaptor.getAllValues().size(), is(1));
                    assertThat(operatorRoleCaptor.getValue(), is(notNullValue()));

                    assertThat(toRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(toCmCaptor.getAllValues().size(), is(1));
                    assertThat(toCmCaptor.getValue(), is(nullValue()));
                    assertThat(uoCmCaptor.getAllValues().size(), is(1));
                    assertThat(uoCmCaptor.getValue(), is(notNullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is("4d715cdd"));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    @Test
    public void reconcileWithoutUoAndTo(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        RoleOperator mockRoleOps = supplier.roleOperations;
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Secret> toSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> uoSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)), uoSecretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Role> operatorRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), operatorRoleCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<RoleBinding> toRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<RoleBinding> uoRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> toCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorLoggingConfigMapName(NAME)), toCmCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<ConfigMap> uoCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorLoggingConfigMapName(NAME)), uoCmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperatorReconciler rcnclr = new EntityOperatorReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(toSecretCaptor.getAllValues().size(), is(1));
                    assertThat(toSecretCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(uoSecretCaptor.getAllValues().size(), is(1));
                    assertThat(uoSecretCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));

                    assertThat(operatorRoleCaptor.getAllValues().size(), is(1));
                    assertThat(operatorRoleCaptor.getValue(), is(nullValue()));

                    assertThat(toRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(toCmCaptor.getAllValues().size(), is(1));
                    assertThat(toCmCaptor.getValue(), is(nullValue()));
                    assertThat(uoCmCaptor.getAllValues().size(), is(1));
                    assertThat(uoCmCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }

    @Test
    public void reconcileWithoutEo(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        RoleOperator mockRoleOps = supplier.roleOperations;
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Secret> toSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorSecretName(NAME)), toSecretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> uoSecretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorSecretName(NAME)), uoSecretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Role> operatorRoleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), operatorRoleCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<RoleBinding> toRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorRoleBinding(NAME)), toRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<RoleBinding> uoRoleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorRoleBinding(NAME)), uoRoleBindingCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> toCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityTopicOperatorLoggingConfigMapName(NAME)), toCmCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<ConfigMap> uoCmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityUserOperatorLoggingConfigMapName(NAME)), uoCmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.entityOperatorDeploymentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        EntityOperatorReconciler rcnclr = new EntityOperatorReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                KAFKA,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(toSecretCaptor.getAllValues().size(), is(1));
                    assertThat(toSecretCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(uoSecretCaptor.getAllValues().size(), is(1));
                    assertThat(uoSecretCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));

                    assertThat(operatorRoleCaptor.getAllValues().size(), is(1));
                    assertThat(operatorRoleCaptor.getValue(), is(nullValue()));

                    assertThat(toRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(toRoleBindingCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(uoRoleBindingCaptor.getAllValues().size(), is(1));
                    assertThat(uoRoleBindingCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(toCmCaptor.getAllValues().size(), is(1));
                    assertThat(toCmCaptor.getValue(), is(nullValue()));
                    assertThat(uoCmCaptor.getAllValues().size(), is(1));
                    assertThat(uoCmCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }
}

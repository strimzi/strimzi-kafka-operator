/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorRbacScopeTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_18;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
    private final ClusterOperatorConfig configNamespaceRbacScope = ResourceUtils.dummyClusterOperatorConfigRolesOnly(
            VERSIONS,
            ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test-ns";
    private final String clusterName = "test-instance";
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
     * Override KafkaAssemblyOperator to only run reconciliation steps that concern the STRIMZI_RBAC_SCOPE feature
     */
    class KafkaAssemblyOperatorRolesSubset extends KafkaAssemblyOperator {
        public KafkaAssemblyOperatorRolesSubset(
                Vertx vertx,
                PlatformFeaturesAvailability pfa,
                CertManager certManager,
                PasswordGenerator passwordGenerator,
                ResourceOperatorSupplier supplier,
                ClusterOperatorConfig config
        ) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.getEntityOperatorDescription()
                    .compose(state -> state.entityOperatorRole())
                    .compose(state -> state.entityUserOperatorRole())
                    .compose(state -> state.entityTopicOperatorRole())
                    .compose(state -> state.entityOperatorServiceAccount())
                    .compose(state -> state.entityOperatorTopicOpRoleBindingForRole())
                    .compose(state -> state.entityOperatorUserOpRoleBindingForRole())
                    .map((Void) null);
        }

    }

    /**
     * This test checks that when STRIMZI_RBAC_SCOPE feature is set to 'NAMESPACE', the cluster operator only
     * deploys and binds to Roles
     */
    @ParallelTest
    public void testRolesDeployedWhenNamespaceRbacScope(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the operations for RoleBindings
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        // Capture the names of reconciled rolebindings and their patched state
        ArgumentCaptor<String> roleBindingNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RoleBinding> roleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(eq(namespace), roleBindingNameCaptor.capture(), roleBindingCaptor.capture()))
                .thenReturn(Future.succeededFuture());

        KafkaAssemblyOperatorRolesSubset kao = new KafkaAssemblyOperatorRolesSubset(
                vertx,
                new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                configNamespaceRbacScope);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<String> roleBindingNames = roleBindingNameCaptor.getAllValues();
                    List<RoleBinding> roleBindings = roleBindingCaptor.getAllValues();

                    assertThat(roleBindingNames, hasSize(2));
                    assertThat(roleBindings, hasSize(2));

                    // Check all RoleBindings, easier to index by order applied
                    assertThat(roleBindingNames.get(0), is("test-instance-entity-topic-operator-role"));
                    assertThat(roleBindings.get(0), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName("test-instance-entity-operator")
                            .build()));

                    assertThat(roleBindingNames.get(1), is("test-instance-entity-user-operator-role"));
                    assertThat(roleBindings.get(1), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName("test-instance-entity-operator")
                            .build()));

                    verify(supplier.clusterRoleBindingOperator, never()).reconcile(anyString(), any());

                    async.flag();
                })));
    }

    /**
     * This test checks that when STRIMZI_RBAC_SCOPE feature is set to 'CLUSTER', the cluster operator
     * binds to ClusterRoles
     */
    @ParallelTest
    public void testRolesDeployedWhenClusterRbacScope(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the operations for RoleBindings
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        // Capture the names of reconciled rolebindings and their patched state
        ArgumentCaptor<String> roleBindingNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RoleBinding> roleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(eq(namespace), roleBindingNameCaptor.capture(), roleBindingCaptor.capture()))
                .thenReturn(Future.succeededFuture());

        KafkaAssemblyOperatorRolesSubset kao = new KafkaAssemblyOperatorRolesSubset(
                vertx,
                new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<String> roleBindingNames = roleBindingNameCaptor.getAllValues();
                    List<RoleBinding> roleBindings = roleBindingCaptor.getAllValues();

                    assertThat(roleBindingNames, hasSize(2));
                    assertThat(roleBindings, hasSize(2));

                    // Check all RoleBindings, easier to index by order applied
                    assertThat(roleBindingNames.get(0), is("test-instance-entity-topic-operator-role"));
                    assertThat(roleBindings.get(0), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName("test-instance-entity-operator")
                            .build()));

                    assertThat(roleBindingNames.get(1), is("test-instance-entity-user-operator-role"));
                    assertThat(roleBindings.get(1), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName("test-instance-entity-operator")
                            .build()));

                    async.flag();
                })));
    }

    /**
     * This test checks that when STRIMZI_RBAC_SCOPE feature is set to 'NAMESPACE', the cluster operator
     * binds to ClusterRoles when it can't use Roles due to cross namespace permissions
     */
    @ParallelTest
    public void testRolesDeployedWhenNamespaceRbacScopeAndMultiWatchNamespace(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                            .withWatchedNamespace("other-ns")
                        .endUserOperator()
                        .withNewTopicOperator()
                            .withWatchedNamespace("another-ns")
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the operations for Roles
        RoleOperator mockRoleOps = supplier.roleOperations;
        // Capture the names of reconciled Roles and their patched state
        ArgumentCaptor<String> roleNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Role> roleCaptor = ArgumentCaptor.forClass(Role.class);
        when(mockRoleOps.reconcile(anyString(), roleNameCaptor.capture(), roleCaptor.capture()))
                .thenReturn(Future.succeededFuture());

        // Mock the operations for RoleBindings
        RoleBindingOperator mockRoleBindingOps = supplier.roleBindingOperations;
        // Capture the names of reconciled RoleBindings and their patched state
        ArgumentCaptor<String> roleBindingNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RoleBinding> roleBindingCaptor = ArgumentCaptor.forClass(RoleBinding.class);
        when(mockRoleBindingOps.reconcile(anyString(), roleBindingNameCaptor.capture(), roleBindingCaptor.capture()))
                .thenReturn(Future.succeededFuture());

        KafkaAssemblyOperatorRolesSubset kao = new KafkaAssemblyOperatorRolesSubset(
                vertx,
                new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                configNamespaceRbacScope);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<String> roleBindingNames = roleBindingNameCaptor.getAllValues();
                    List<RoleBinding> roleBindings = roleBindingCaptor.getAllValues();

                    assertThat(roleBindingNames, hasSize(4));
                    assertThat(roleBindings, hasSize(4));


                    // Check all RoleBindings, easier to index by order applied
                    assertThat(roleBindingNames.get(0), is("test-instance-entity-topic-operator-role"));
                    assertThat(roleBindings.get(0).getMetadata().getNamespace(), is("another-ns"));
                    assertThat(roleBindings.get(0), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName(EntityOperator.getRoleName(clusterName))
                            .build()));

                    assertThat(roleBindingNames.get(1), is("test-instance-entity-topic-operator-role"));
                    assertThat(roleBindings.get(1).getMetadata().getNamespace(), is("test-ns"));
                    assertThat(roleBindings.get(1), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName(EntityOperator.getRoleName(clusterName))
                            .build()));

                    assertThat(roleBindingNames.get(2), is("test-instance-entity-user-operator-role"));
                    assertThat(roleBindings.get(2).getMetadata().getNamespace(), is("other-ns"));
                    assertThat(roleBindings.get(2), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName(EntityOperator.getRoleName(clusterName))
                            .build()));

                    assertThat(roleBindingNames.get(3), is("test-instance-entity-user-operator-role"));
                    assertThat(roleBindings.get(3).getMetadata().getNamespace(), is("test-ns"));
                    assertThat(roleBindings.get(3), hasRoleRef(new RoleRefBuilder()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("Role")
                            .withName(EntityOperator.getRoleName(clusterName))
                            .build()));

                    List<String> roleNames = roleNameCaptor.getAllValues();
                    List<Role> roles = roleCaptor.getAllValues();

                    assertThat(roleNames, hasSize(3));
                    assertThat(roles, hasSize(3));

                    // Check all Roles, easier to index by order applied
                    assertThat(roleNames.get(0), is("test-instance-entity-operator"));
                    assertThat(roles.get(0).getMetadata().getNamespace(), is("test-ns"));

                    assertThat(roleNames.get(1), is("test-instance-entity-operator"));
                    assertThat(roles.get(1).getMetadata().getNamespace(), is("other-ns"));

                    assertThat(roleNames.get(2), is("test-instance-entity-operator"));
                    assertThat(roles.get(2).getMetadata().getNamespace(), is("another-ns"));

                    async.flag();
                })));
    }

    public static Matcher<RoleBinding> hasRoleRef(RoleRef roleRef) {
        return new TypeSafeDiagnosingMatcher<RoleBinding>() {

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected Role Reference ").appendValue(roleRef);
            }

            @Override
            protected boolean matchesSafely(RoleBinding actual, Description mismatchDescription) {
                boolean matches = roleRef.equals(actual.getRoleRef());
                if (!matches) {
                    mismatchDescription.appendText(" was ").appendValue(actual.getRoleRef())
                    .appendText(" in ").appendValue(actual);
                }

                return matches;
            }
        };
    }

}

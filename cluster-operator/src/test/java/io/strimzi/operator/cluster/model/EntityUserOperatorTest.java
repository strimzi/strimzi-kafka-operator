/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorization;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationCustomBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpa;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class EntityUserOperatorTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME)
                .withLabels(Map.of("my-user-label", "cromulent"))
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
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
                .withNewEntityOperator()
                    .withNewUserOperator()
                        .withWatchedNamespace("my-user-namespace")
                        .withImage("my-image:latest")
                        .withReconciliationIntervalMs(60_000L)
                        .withSecretPrefix("strimzi-")
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(15)
                            .withTimeoutSeconds(20)
                            .withFailureThreshold(12)
                            .withSuccessThreshold(5)
                            .withPeriodSeconds(180)
                        .endLivenessProbe()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(15)
                            .withTimeoutSeconds(20)
                            .withFailureThreshold(12)
                            .withSuccessThreshold(5)
                            .withPeriodSeconds(180)
                        .endReadinessProbe()
                        .withNewInlineLogging()
                            .withLoggers(Map.of("user-operator.root.logger", "OFF"))
                        .endInlineLogging()
                        .withNewJvmOptions()
                                .withXmx("256m")
                                .addAllToJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                        new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                        .endJvmOptions()
                    .endUserOperator()
                    .withNewTemplate()
                        .withNewUserOperatorRoleBinding()
                            .withNewMetadata()
                                .withLabels(Map.of("label-1", "value-1"))
                                .withAnnotations(Map.of("anno-1", "value-1"))
                            .endMetadata()
                        .endUserOperatorRoleBinding()
                    .endTemplate()
                .endEntityOperator()
            .endSpec()
            .build();
    private static final EntityUserOperator EUO = EntityUserOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), KAFKA, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

    @ParallelTest
    public void testEnvVars()   {
        checkEnvVars(getExpectedEnvVars(), EUO.getEnvVars());
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(EUO.namespace, is(NAMESPACE));
        assertThat(EUO.cluster, is(CLUSTER_NAME));
        assertThat(EUO.image, is("my-image:latest"));
        assertThat(EUO.readinessProbeOptions.getInitialDelaySeconds(), is(15));
        assertThat(EUO.readinessProbeOptions.getTimeoutSeconds(), is(20));
        assertThat(EUO.readinessProbeOptions.getSuccessThreshold(), is(5));
        assertThat(EUO.readinessProbeOptions.getFailureThreshold(), is(12));
        assertThat(EUO.readinessProbeOptions.getPeriodSeconds(), is(180));
        assertThat(EUO.livenessProbeOptions.getInitialDelaySeconds(), is(15));
        assertThat(EUO.livenessProbeOptions.getTimeoutSeconds(), is(20));
        assertThat(EUO.livenessProbeOptions.getSuccessThreshold(), is(5));
        assertThat(EUO.livenessProbeOptions.getFailureThreshold(), is(12));
        assertThat(EUO.livenessProbeOptions.getPeriodSeconds(), is(180));
        assertThat(EUO.watchedNamespace(), is("my-user-namespace"));
        assertThat(EUO.reconciliationIntervalMs, is(60_000L));
        assertThat(EUO.kafkaBootstrapServers, is(String.format("%s:%d", KafkaResources.bootstrapServiceName(CLUSTER_NAME), EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT)));
        assertThat(EUO.logging().getLogging().getType(), is(InlineLogging.TYPE_INLINE));
        assertThat(((InlineLogging) EUO.logging().getLogging()).getLoggers(), is(Map.of("user-operator.root.logger", "OFF")));
        assertThat(EUO.secretPrefix, is("strimzi-"));
    }

    @ParallelTest
    public void testPeriodicReconciliationIntervalConfig() {
        // default value
        EntityUserOperatorSpec entityUserOperatorSpec0 = new EntityUserOperatorSpecBuilder().build();
        EntityUserOperator entityUserOperator0 = buildEntityUserOperatorWithReconciliationInterval(entityUserOperatorSpec0);
        assertThat(entityUserOperator0.reconciliationIntervalMs, nullValue());

        // new config (ms)
        EntityUserOperatorSpec entityUserOperatorSpec1 = new EntityUserOperatorSpecBuilder().withReconciliationIntervalMs(10_000L).build();
        EntityUserOperator entityUserOperator1 = buildEntityUserOperatorWithReconciliationInterval(entityUserOperatorSpec1);
        assertThat(entityUserOperator1.reconciliationIntervalMs, is(10_000L));

        // legacy config (seconds)
        EntityUserOperatorSpec entityUserOperatorSpec2 = new EntityUserOperatorSpecBuilder().withReconciliationIntervalSeconds(15L).build();
        EntityUserOperator entityUserOperator2 = buildEntityUserOperatorWithReconciliationInterval(entityUserOperatorSpec2);
        assertThat(entityUserOperator2.reconciliationIntervalMs, is(15_000L));

        // both (new config should prevail)
        EntityUserOperatorSpec entityUserOperatorSpec3 = new EntityUserOperatorSpecBuilder()
            .withReconciliationIntervalMs(10_000L).withReconciliationIntervalSeconds(15L).build();
        EntityUserOperator entityUserOperator3 = buildEntityUserOperatorWithReconciliationInterval(entityUserOperatorSpec3);
        assertThat(entityUserOperator3.reconciliationIntervalMs, is(10_000L));
    }

    private EntityUserOperator buildEntityUserOperatorWithReconciliationInterval(EntityUserOperatorSpec userOperatorSpec) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                .withNewEntityOperator()
                    .withUserOperator(userOperatorSpec)
                .endEntityOperator()
                .endSpec()
                .build();

        return EntityUserOperator.fromCrd(new Reconciliation("test", kafka.getKind(), kafka.getMetadata().getNamespace(), kafka.getMetadata().getName()), kafka, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());
    }

    @ParallelTest
    public void testFromCrdDefault() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityUserOperator.watchedNamespace(), is(NAMESPACE));
        assertThat(entityUserOperator.getImage(), is("quay.io/strimzi/operator:latest"));
        assertThat(entityUserOperator.reconciliationIntervalMs, nullValue());
        assertThat(entityUserOperator.readinessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityUserOperator.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityUserOperator.livenessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityUserOperator.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityUserOperator.kafkaBootstrapServers, is(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT));
        assertThat(entityUserOperator.logging().getLogging(), is(nullValue()));
        assertThat(entityUserOperator.secretPrefix, is(EntityUserOperatorSpec.DEFAULT_SECRET_PREFIX));
    }

    @ParallelTest
    public void testFromCrdNoEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withEntityOperator(null)
                .endSpec()
                .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityUserOperator, is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoUserOperatorInEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityUserOperator, is(nullValue()));
    }

    @ParallelTest
    public void testGetContainers() {
        Container container = EUO.createContainer(null);
        assertThat(container.getName(), is(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME));
        assertThat(container.getImage(), is(EUO.getImage()));
        checkEnvVars(getExpectedEnvVars(), container.getEnv());
        assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(15));
        assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(20));
        assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(15));
        assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(20));
        assertThat(container.getPorts().size(), is(1));
        assertThat(container.getPorts().get(0).getContainerPort(), is(EntityUserOperator.HEALTHCHECK_PORT));
        assertThat(container.getPorts().get(0).getName(), is(EntityUserOperator.HEALTHCHECK_PORT_NAME));
        assertThat(container.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(Map.of(
                EntityUserOperator.USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
                "entity-user-operator-metrics-and-logging", "/opt/user-operator/custom-config/",
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.EUO_CERTS_VOLUME_NAME, EntityOperator.EUO_CERTS_VOLUME_MOUNT)));
    }

    @ParallelTest
    public void testFromCrdCaValidityAndRenewal() {
        Kafka customValues = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                    .withNewClientsCa()
                        .withValidityDays(42)
                        .withRenewalDays(69)
                    .endClientsCa()
                .endSpec()
                .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), customValues, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        Kafka defaultValues = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityUserOperator entityUserOperator2 = EntityUserOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), defaultValues, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityUserOperator.clientsCaValidityDays, is(42));
        assertThat(entityUserOperator.clientsCaRenewalDays, is(69));
        assertThat(entityUserOperator2.clientsCaValidityDays, is(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS));
        assertThat(entityUserOperator2.clientsCaRenewalDays, is(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS));

        List<EnvVar> envVars = entityUserOperator.getEnvVars();
        List<EnvVar> envVars2 = entityUserOperator2.getEnvVars();
        assertThat(Integer.parseInt(envVars.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY)).findFirst().orElseThrow().getValue()), is(42));
        assertThat(Integer.parseInt(envVars.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL)).findFirst().orElseThrow().getValue()), is(69));
        assertThat(Integer.parseInt(envVars2.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY)).findFirst().orElseThrow().getValue()), is(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS));
        assertThat(Integer.parseInt(envVars2.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL)).findFirst().orElseThrow().getValue()), is(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS));
    }

    @ParallelTest
    public void testRoleBindingInOtherNamespace()   {
        RoleBinding binding = EUO.generateRoleBindingForRole(NAMESPACE, "my-user-namespace");

        assertThat(binding.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(binding.getMetadata().getNamespace(), is("my-user-namespace"));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(0));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("my-cluster-entity-operator"));
    }

    @ParallelTest
    public void testRoleBindingInTheSameNamespace() {
        RoleBinding binding = EUO.generateRoleBindingForRole(NAMESPACE, NAMESPACE);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(binding.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("my-cluster-entity-operator"));
    }

    @SuppressWarnings("deprecation") // OPA Authorization is deprecated
    @ParallelTest
    public void testAclsAdminApiSupported() {
        testAclsAdminApiSupported(new KafkaAuthorizationSimple());
        testAclsAdminApiSupported(new KafkaAuthorizationOpa());
        testAclsAdminApiSupported(new KafkaAuthorizationKeycloakBuilder().withDelegateToKafkaAcls(true).build());
        testAclsAdminApiSupported(new KafkaAuthorizationKeycloakBuilder().withDelegateToKafkaAcls(false).build());
        testAclsAdminApiSupported(new KafkaAuthorizationCustomBuilder().withSupportsAdminApi(true).build());
        testAclsAdminApiSupported(new KafkaAuthorizationCustomBuilder().withSupportsAdminApi(false).build());
    }

    private void testAclsAdminApiSupported(KafkaAuthorization authorizer) {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withAuthorization(authorizer)
                    .endKafka()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityUserOperator f = EntityUserOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(f.getEnvVars()
                    .stream()
                    .filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_ACLS_ADMIN_API_SUPPORTED))
                    .findFirst()
                    .orElseThrow()
                    .getValue(),
                is(String.valueOf(authorizer.supportsAdminApi())));
    }

    @ParallelTest
    public void testMaintenanceTimeWindows()    {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityUserOperator f = EntityUserOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(f.getEnvVars().stream().anyMatch(a -> EntityUserOperator.ENV_VAR_MAINTENANCE_TIME_WINDOWS.equals(a.getName())), is(false));

        kafkaAssembly = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .withMaintenanceTimeWindows("* * 8-10 * * ?", "* * 14-15 * * ?")
                .endSpec()
                .build();
        f = EntityUserOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(f.getEnvVars().stream().filter(a -> EntityUserOperator.ENV_VAR_MAINTENANCE_TIME_WINDOWS.equals(a.getName())).findFirst().orElseThrow().getValue(), is("* * 8-10 * * ?;* * 14-15 * * ?"));
    }

    @ParallelTest
    public void testNoWatchedNamespace() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityUserOperator.watchedNamespace(), is(NAMESPACE));
    }

    @ParallelTest
    public void testWatchedNamespace() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                            .withWatchedNamespace("some-other-namespace")
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityUserOperator.watchedNamespace(), is("some-other-namespace"));
    }

    ////////////////////
    // Utility methods
    ////////////////////

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(String.format("%s:%d", "my-cluster-kafka-bootstrap", EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_WATCHED_NAMESPACE).withValue("my-user-namespace").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(60_000L)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME).withValue(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME).withValue(KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_NAMESPACE).withValue(NAMESPACE).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME).withValue(KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_EO_KEY_SECRET_NAME).withValue(KafkaResources.entityUserOperatorSecretName(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_EO_KEY_NAME).withValue(KafkaResources.entityUserOperatorKeyName(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_JAVA_OPTS).withValue("-Xmx256m").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue("-Djavax.net.debug=verbose -Dsomething.else=42").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_SECRET_PREFIX).withValue("strimzi-").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_ACLS_ADMIN_API_SUPPORTED).withValue(String.valueOf(false)).build());

        return expected;
    }

    private void checkEnvVars(List<EnvVar> expected, List<EnvVar> actual)   {
        assertThat(actual.size(), is(expected.size()));

        for (EnvVar var : expected) {
            assertThat(actual.contains(var), is(true));
        }
    }
}

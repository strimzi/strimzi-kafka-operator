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
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.SystemProperty;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorization;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationCustomBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpa;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpecBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.TestUtils.map;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class EntityUserOperatorTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final InlineLogging userOperatorLogging = new InlineLogging();
    {
        userOperatorLogging.setLoggers(Collections.singletonMap("user-operator.root.logger", "OFF"));
    }
    private final Probe livenessProbe = new Probe();
    {
        livenessProbe.setInitialDelaySeconds(15);
        livenessProbe.setTimeoutSeconds(20);
        livenessProbe.setFailureThreshold(12);
        livenessProbe.setSuccessThreshold(5);
        livenessProbe.setPeriodSeconds(180);
    }

    private final Probe readinessProbe = new Probe();
    {
        readinessProbe.setInitialDelaySeconds(15);
        livenessProbe.setInitialDelaySeconds(20);
        readinessProbe.setFailureThreshold(12);
        readinessProbe.setSuccessThreshold(5);
        readinessProbe.setPeriodSeconds(180);
    }

    private final String uoWatchedNamespace = "my-user-namespace";
    private final String uoImage = "my-user-operator-image";
    private final String secretPrefix = "strimzi-";
    private final int uoReconciliationInterval = 120;

    private final String metricsCMName = "metrics-cm";
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics(MetricsModel.CONFIG_MAP_KEY, metricsCMName);
    private final List<SystemProperty> javaSystemProperties = new ArrayList<>() {{
            add(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build());
            add(new SystemPropertyBuilder().withName("something.else").withValue("42").build());
        }};

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .withWatchedNamespace(uoWatchedNamespace)
            .withImage(uoImage)
            .withReconciliationIntervalSeconds(uoReconciliationInterval)
            .withSecretPrefix(secretPrefix)
            .withLivenessProbe(livenessProbe)
            .withReadinessProbe(readinessProbe)
            .withLogging(userOperatorLogging)
            .withNewJvmOptions()
                .addAllToJavaSystemProperties(javaSystemProperties)
                .withXmx("256m")
            .endJvmOptions()
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withUserOperator(entityUserOperatorSpec)
            .withTemplate(new EntityOperatorTemplateBuilder()
                    .withNewUserOperatorRoleBinding()
                        .withNewMetadata()
                            .withLabels(Map.of("label-1", "value-1"))
                            .withAnnotations(Map.of("anno-1", "value-1"))
                        .endMetadata()
                    .endUserOperatorRoleBinding()
                    .build())
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(String.format("%s:%d", "foo-kafka-bootstrap", EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(uoWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(uoReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME).withValue(KafkaResources.clientsCaKeySecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME).withValue(KafkaResources.clientsCaCertificateSecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_NAMESPACE).withValue(namespace).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME).withValue(KafkaCluster.clusterCaCertSecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_EO_KEY_SECRET_NAME).withValue(KafkaResources.entityUserOperatorSecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_JAVA_OPTS).withValue("-Xmx256m").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue("-Djavax.net.debug=verbose -Dsomething.else=42").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_SECRET_PREFIX).withValue(secretPrefix).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_ACLS_ADMIN_API_SUPPORTED).withValue(String.valueOf(false)).build());

        return expected;
    }

    private void checkEnvVars(List<EnvVar> expected, List<EnvVar> actual)   {
        assertThat(actual.size(), is(expected.size()));

        for (EnvVar var : expected) {
            assertThat(actual.contains(var), is(true));
        }
    }

    @ParallelTest
    public void testEnvVars()   {
        checkEnvVars(getExpectedEnvVars(), entityUserOperator.getEnvVars());
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(entityUserOperator.namespace, is(namespace));
        assertThat(entityUserOperator.cluster, is(cluster));
        assertThat(entityUserOperator.image, is(uoImage));
        assertThat(entityUserOperator.readinessProbeOptions.getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
        assertThat(entityUserOperator.readinessProbeOptions.getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
        assertThat(entityUserOperator.readinessProbeOptions.getSuccessThreshold(), is(readinessProbe.getSuccessThreshold()));
        assertThat(entityUserOperator.readinessProbeOptions.getFailureThreshold(), is(readinessProbe.getFailureThreshold()));
        assertThat(entityUserOperator.readinessProbeOptions.getPeriodSeconds(), is(readinessProbe.getPeriodSeconds()));
        assertThat(entityUserOperator.livenessProbeOptions.getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
        assertThat(entityUserOperator.livenessProbeOptions.getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
        assertThat(entityUserOperator.livenessProbeOptions.getSuccessThreshold(), is(livenessProbe.getSuccessThreshold()));
        assertThat(entityUserOperator.livenessProbeOptions.getFailureThreshold(), is(livenessProbe.getFailureThreshold()));
        assertThat(entityUserOperator.livenessProbeOptions.getPeriodSeconds(), is(livenessProbe.getPeriodSeconds()));
        assertThat(entityUserOperator.watchedNamespace(), is(uoWatchedNamespace));
        assertThat(entityUserOperator.reconciliationIntervalMs, is(uoReconciliationInterval * 1000L));
        assertThat(entityUserOperator.kafkaBootstrapServers, is(String.format("%s:%d", KafkaResources.bootstrapServiceName(cluster), EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT)));
        assertThat(entityUserOperator.logging().getLogging().getType(), is(userOperatorLogging.getType()));
        assertThat(((InlineLogging) entityUserOperator.logging().getLogging()).getLoggers(), is(userOperatorLogging.getLoggers()));
        assertThat(entityUserOperator.secretPrefix, is(secretPrefix));
    }

    @ParallelTest
    public void testFromCrdDefault() {
        EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withUserOperator(entityUserOperatorSpec)
                .build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityUserOperator.watchedNamespace(), is(namespace));
        assertThat(entityUserOperator.getImage(), is("quay.io/strimzi/operator:latest"));
        assertThat(entityUserOperator.reconciliationIntervalMs, is(EntityUserOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000));
        assertThat(entityUserOperator.readinessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityUserOperator.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityUserOperator.livenessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityUserOperator.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityUserOperator.kafkaBootstrapServers, is(KafkaResources.bootstrapServiceName(cluster) + ":" + EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT));
        assertThat(entityUserOperator.logging().getLogging(), is(nullValue()));
        assertThat(entityUserOperator.secretPrefix, is(EntityUserOperatorSpec.DEFAULT_SECRET_PREFIX));
    }

    @ParallelTest
    public void testFromCrdNoEntityOperator() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout);

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityUserOperator, is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityUserOperator, is(nullValue()));
    }

    @ParallelTest
    public void testGetContainers() {
        Container container = entityUserOperator.createContainer(null);
        assertThat(container.getName(), is(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME));
        assertThat(container.getImage(), is(entityUserOperator.getImage()));
        checkEnvVars(getExpectedEnvVars(), container.getEnv());
        assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
        assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
        assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
        assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
        assertThat(container.getPorts().size(), is(1));
        assertThat(container.getPorts().get(0).getContainerPort(), is(EntityUserOperator.HEALTHCHECK_PORT));
        assertThat(container.getPorts().get(0).getName(), is(EntityUserOperator.HEALTHCHECK_PORT_NAME));
        assertThat(container.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(map(
                EntityUserOperator.USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
                "entity-user-operator-metrics-and-logging", "/opt/user-operator/custom-config/",
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.EUO_CERTS_VOLUME_NAME, EntityOperator.EUO_CERTS_VOLUME_MOUNT)));
    }

    @ParallelTest
    public void testFromCrdCaValidityAndRenewal() {
        EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withUserOperator(entityUserOperatorSpec)
                .build();
        CertificateAuthority ca = new CertificateAuthority();
        ca.setValidityDays(42);
        ca.setRenewalDays(69);
        Kafka customValues =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .withClientsCa(ca)
                        .endSpec()
                        .build();

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), customValues, SHARED_ENV_PROVIDER);

        Kafka defaultValues =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();

        EntityUserOperator entityUserOperator2 = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), defaultValues, SHARED_ENV_PROVIDER);

        assertThat(entityUserOperator.clientsCaValidityDays, is(42));
        assertThat(entityUserOperator.clientsCaRenewalDays, is(69));
        assertThat(entityUserOperator2.clientsCaValidityDays, is(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS));
        assertThat(entityUserOperator2.clientsCaRenewalDays, is(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS));
    }

    @ParallelTest
    public void testEntityUserOperatorEnvVarValidityAndRenewal() {
        int validity = 100;
        int renewal = 42;

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, singletonMap("foo", "bar"), emptyMap()))
                .editSpec()
                .withNewClientsCa()
                .withRenewalDays(renewal)
                .withValidityDays(validity)
                .endClientsCa()
                .withNewEntityOperator()
                .withNewUserOperator()
                .endUserOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityUserOperator f = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER);

        List<EnvVar> envvar = f.getEnvVars();
        assertThat(Integer.parseInt(envvar.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY)).findFirst().orElseThrow().getValue()), is(validity));
        assertThat(Integer.parseInt(envvar.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL)).findFirst().orElseThrow().getValue()), is(renewal));
    }

    @ParallelTest
    public void testRoleBindingInOtherNamespace()   {
        RoleBinding binding = entityUserOperator.generateRoleBindingForRole(namespace, uoWatchedNamespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(uoWatchedNamespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(0));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("foo-entity-operator"));
    }

    @ParallelTest
    public void testRoleBindingInTheSameNamespace() {
        RoleBinding binding = entityUserOperator.generateRoleBindingForRole(namespace, namespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("foo-entity-operator"));
    }

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
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, singletonMap("foo", "bar"), emptyMap()))
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

        EntityUserOperator f = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER);

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
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, singletonMap("foo", "bar"), emptyMap()))
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityUserOperator f = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER);
        assertThat(f.getEnvVars().stream().anyMatch(a -> EntityUserOperator.ENV_VAR_MAINTENANCE_TIME_WINDOWS.equals(a.getName())), is(false));

        kafkaAssembly = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .withMaintenanceTimeWindows("* * 8-10 * * ?", "* * 14-15 * * ?")
                .endSpec()
                .build();

        f = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), kafkaAssembly, SHARED_ENV_PROVIDER);
        assertThat(f.getEnvVars().stream().filter(a -> EntityUserOperator.ENV_VAR_MAINTENANCE_TIME_WINDOWS.equals(a.getName())).findFirst().orElseThrow().getValue(), is("* * 8-10 * * ?;* * 14-15 * * ?"));
    }

    @ParallelTest
    public void testNoWatchedNamespace() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                .endSpec()
                .build();

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityUserOperator.watchedNamespace(), is(namespace));
    }

    @ParallelTest
    public void testWatchedNamespace() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                    .withWatchedNamespace("some-other-namespace")
                .endUserOperator()
                .build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                .endSpec()
                .build();

        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityUserOperator.watchedNamespace(), is("some-other-namespace"));
    }
}

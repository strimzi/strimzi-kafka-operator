/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.RackBuilder;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.EntityTopicOperator.CRUISE_CONTROL_API_PORT;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EntityTopicOperatorTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME)
                .withLabels(Map.of("my-user-label", "cromulent"))
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
                    .withNewTopicOperator()
                        .withWatchedNamespace("my-topic-namespace")
                        .withImage("my-image:latest")
                        .withReconciliationIntervalMs(60_000L)
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
                            .withLoggers(Map.of("topic-operator.root.logger", "OFF"))
                        .endInlineLogging()
                        .withNewJvmOptions()
                                .withXms("128m")
                                .addAllToJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                        new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                        .endJvmOptions()
                    .endTopicOperator()
                    .withNewTemplate()
                        .withNewTopicOperatorRoleBinding()
                            .withNewMetadata()
                                .withLabels(Map.of("label-1", "value-1"))
                                .withAnnotations(Map.of("anno-1", "value-1"))
                            .endMetadata()
                        .endTopicOperatorRoleBinding()
                    .endTemplate()
                .endEntityOperator()
            .endSpec()
            .build();
    private static final EntityTopicOperator ETO = EntityTopicOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), KAFKA, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

    @Test
    public void testEnvVars()   {
        assertThat(ETO.getEnvVars(), is(getExpectedEnvVars()));
    }

    @Test
    public void testFromCrd() {
        assertThat(ETO.namespace, is(NAMESPACE));
        assertThat(ETO.cluster, is(CLUSTER_NAME));
        assertThat(ETO.image, is("my-image:latest"));
        assertThat(ETO.readinessProbeOptions.getInitialDelaySeconds(), is(15));
        assertThat(ETO.readinessProbeOptions.getTimeoutSeconds(), is(20));
        assertThat(ETO.readinessProbeOptions.getSuccessThreshold(), is(5));
        assertThat(ETO.readinessProbeOptions.getFailureThreshold(), is(12));
        assertThat(ETO.readinessProbeOptions.getPeriodSeconds(), is(180));
        assertThat(ETO.livenessProbeOptions.getInitialDelaySeconds(), is(15));
        assertThat(ETO.livenessProbeOptions.getTimeoutSeconds(), is(20));
        assertThat(ETO.livenessProbeOptions.getSuccessThreshold(), is(5));
        assertThat(ETO.livenessProbeOptions.getFailureThreshold(), is(12));
        assertThat(ETO.livenessProbeOptions.getPeriodSeconds(), is(180));
        assertThat(ETO.watchedNamespace(), is("my-topic-namespace"));
        assertThat(ETO.reconciliationIntervalMs, is(60_000L));
        assertThat(ETO.kafkaBootstrapServers, is(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT));
        assertThat(ETO.resourceLabels, is(ModelUtils.defaultResourceLabels(CLUSTER_NAME)));
        assertThat(ETO.logging().getLogging().getType(), is(InlineLogging.TYPE_INLINE));
        assertThat(((InlineLogging) ETO.logging().getLogging()).getLoggers(), is(Map.of("topic-operator.root.logger", "OFF")));
    }

    @Test
    public void testPeriodicReconciliationIntervalConfig() {
        // default value
        EntityTopicOperatorSpec entityTopicOperatorSpec0 = new EntityTopicOperatorSpecBuilder().build();
        EntityTopicOperator entityTopicOperator0 = buildEntityTopicOperatorWithReconciliationInterval(entityTopicOperatorSpec0);
        assertThat(entityTopicOperator0.reconciliationIntervalMs, nullValue());
        
        // new config (ms)
        EntityTopicOperatorSpec entityTopicOperatorSpec1 = new EntityTopicOperatorSpecBuilder().withReconciliationIntervalMs(10_000L).build();
        EntityTopicOperator entityTopicOperator1 = buildEntityTopicOperatorWithReconciliationInterval(entityTopicOperatorSpec1);
        assertThat(entityTopicOperator1.reconciliationIntervalMs, is(10_000L));
        
        // legacy config (seconds)
        EntityTopicOperatorSpec entityTopicOperatorSpec2 = new EntityTopicOperatorSpecBuilder().withReconciliationIntervalSeconds(15).build();
        EntityTopicOperator entityTopicOperator2 = buildEntityTopicOperatorWithReconciliationInterval(entityTopicOperatorSpec2);
        assertThat(entityTopicOperator2.reconciliationIntervalMs, is(15_000L));
        
        // both (new config should prevail)
        EntityTopicOperatorSpec entityTopicOperatorSpec3 = new EntityTopicOperatorSpecBuilder()
            .withReconciliationIntervalMs(10_000L).withReconciliationIntervalSeconds(15).build();
        EntityTopicOperator entityTopicOperator3 = buildEntityTopicOperatorWithReconciliationInterval(entityTopicOperatorSpec3);
        assertThat(entityTopicOperator3.reconciliationIntervalMs, is(10_000L));
    }
    
    private EntityTopicOperator buildEntityTopicOperatorWithReconciliationInterval(EntityTopicOperatorSpec topicOperatorSpec) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(topicOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();

        return EntityTopicOperator.fromCrd(new Reconciliation("test", kafka.getKind(), kafka.getMetadata().getNamespace(), kafka.getMetadata().getName()), kafka, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());
    }

    @Test
    public void testFromCrdDefault() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityTopicOperator.watchedNamespace(), is(NAMESPACE));
        assertThat(entityTopicOperator.getImage(), is("quay.io/strimzi/operator:latest"));
        assertThat(entityTopicOperator.reconciliationIntervalMs, nullValue());
        assertThat(entityTopicOperator.kafkaBootstrapServers, is(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT));
        assertThat(entityTopicOperator.resourceLabels, is(ModelUtils.defaultResourceLabels(CLUSTER_NAME)));
        assertThat(entityTopicOperator.readinessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityTopicOperator.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityTopicOperator.livenessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityTopicOperator.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityTopicOperator.logging().getLogging(), is(nullValue()));
    }

    @Test
    public void testFromCrdNoEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withEntityOperator(null)
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityTopicOperator, is(nullValue()));
    }

    @Test
    public void testFromCrdNoTopicOperatorInEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityTopicOperator, is(nullValue()));
    }

    @Test
    public void testWatchedNamespace() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withWatchedNamespace("some-other-namespace")
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityTopicOperator.watchedNamespace(), is("some-other-namespace"));
    }

    @Test
    public void testGetContainers() {
        Container container = ETO.createContainer(null);
        assertThat(container.getName(), is(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME));
        assertThat(container.getImage(), is(ETO.getImage()));
        assertThat(container.getEnv(), is(getExpectedEnvVars()));
        assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(15));
        assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(20));
        assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(15));
        assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(20));
        assertThat(container.getPorts().size(), is(1));
        assertThat(container.getPorts().get(0).getContainerPort(), is(EntityTopicOperator.HEALTHCHECK_PORT));
        assertThat(container.getPorts().get(0).getName(), is(EntityTopicOperator.HEALTHCHECK_PORT_NAME));
        assertThat(container.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(Map.of(
                EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
                "entity-topic-operator-metrics-and-logging", "/opt/topic-operator/custom-config/",
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.ETO_CERTS_VOLUME_NAME, EntityOperator.ETO_CERTS_VOLUME_MOUNT)));
    }

    @Test
    public void testRoleBindingInOtherNamespace()   {
        RoleBinding binding = ETO.generateRoleBindingForRole(NAMESPACE, "my-topic-namespace");

        assertThat(binding.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(binding.getMetadata().getNamespace(), is("my-topic-namespace"));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(0));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("my-cluster-entity-operator"));
    }

    @Test
    public void testRoleBindingInTheSameNamespace()   {
        RoleBinding binding = ETO.generateRoleBindingForRole(NAMESPACE, NAMESPACE);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(binding.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("my-cluster-entity-operator"));
    }

    @Test
    public void testSetupWithCruiseControlEnabled() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withRack(new RackBuilder().withTopologyKey("foo").build())
                    .endKafka()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        List<EnvVar> expectedEnvVars = new ArrayList<>();
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(CLUSTER_NAME)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(NAMESPACE).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_SECURITY_PROTOCOL).withValue(EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_RACK_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_HOSTNAME).withValue(String.format("%s-cruise-control.%s.svc", CLUSTER_NAME, NAMESPACE)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_PORT).withValue(String.valueOf(CRUISE_CONTROL_API_PORT)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_SSL_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_AUTH_ENABLED).withValue(Boolean.toString(true)).build());
        assertThat(entityTopicOperator.getEnvVars(), is(expectedEnvVars));

        Container container = entityTopicOperator.createContainer(null);
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(Map.of(
            EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
            "entity-topic-operator-metrics-and-logging", "/opt/topic-operator/custom-config/",
            EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
            EntityOperator.ETO_CERTS_VOLUME_NAME, EntityOperator.ETO_CERTS_VOLUME_MOUNT,
            EntityOperator.ETO_CC_API_VOLUME_NAME, EntityOperator.ETO_CC_API_VOLUME_MOUNT
        )));
    }

    @Test
    public void testGenerateCruiseControlApiSecret() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        Secret newSecret = entityTopicOperator.generateCruiseControlApiSecret(null);
        assertThat(newSecret, is(notNullValue()));
        assertThat(newSecret.getData(), is(notNullValue()));
        assertThat(newSecret.getData().size(), is(2));
        assertThat(newSecret.getData().get(TOPIC_OPERATOR_USERNAME_KEY), is(Util.encodeToBase64(TOPIC_OPERATOR_USERNAME)));
        assertThat(newSecret.getData().get(TOPIC_OPERATOR_USERNAME_KEY), is(notNullValue()));
        
        String name = Util.encodeToBase64(TOPIC_OPERATOR_USERNAME);
        String password = Util.encodeToBase64("change-it");
        Secret oldSecret = entityTopicOperator.generateCruiseControlApiSecret(new SecretBuilder().withData(Map.of(TOPIC_OPERATOR_USERNAME_KEY, name, TOPIC_OPERATOR_PASSWORD_KEY, password)).build());
        assertThat(oldSecret, is(notNullValue()));
        assertThat(oldSecret.getData(), is(notNullValue()));
        assertThat(oldSecret.getData().size(), is(2));
        assertThat(oldSecret.getData().get(TOPIC_OPERATOR_USERNAME_KEY), is(name));
        assertThat(oldSecret.getData().get(TOPIC_OPERATOR_PASSWORD_KEY), is(password));
        
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(new SecretBuilder().withData(Map.of(TOPIC_OPERATOR_USERNAME_KEY, name)).build()));
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(new SecretBuilder().withData(Map.of(TOPIC_OPERATOR_PASSWORD_KEY, password)).build()));
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(new SecretBuilder().withData(Map.of()).build()));
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(new SecretBuilder().withData(Map.of(TOPIC_OPERATOR_USERNAME_KEY, " ", TOPIC_OPERATOR_PASSWORD_KEY, password)).build()));
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(new SecretBuilder().withData(Map.of(TOPIC_OPERATOR_USERNAME_KEY, name, TOPIC_OPERATOR_PASSWORD_KEY, " ")).build()));
    }

    ////////////////////
    // Utility methods
    ////////////////////

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(CLUSTER_NAME)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue("my-topic-namespace").build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(60000)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_SECURITY_PROTOCOL).withValue(EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_JAVA_OPTS).withValue("-Xms128m").build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue("-Djavax.net.debug=verbose -Dsomething.else=42").build());
        return expected;
    }
}

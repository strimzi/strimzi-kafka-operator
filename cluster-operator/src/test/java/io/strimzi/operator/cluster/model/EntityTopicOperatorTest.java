/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.RackBuilder;
import io.strimzi.api.kafka.model.common.SystemProperty;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpecBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.EntityTopicOperator.CRUISE_CONTROL_API_PORT;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_TO_ADMIN_NAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_TO_ADMIN_NAME_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_TO_ADMIN_PASSWORD_KEY;
import static io.strimzi.test.TestUtils.map;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class EntityTopicOperatorTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final InlineLogging topicOperatorLogging = new InlineLogging();
    {
        topicOperatorLogging.setLoggers(Collections.singletonMap("topic-operator.root.logger", "OFF"));
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
        readinessProbe.setInitialDelaySeconds(20);
        readinessProbe.setFailureThreshold(12);
        readinessProbe.setSuccessThreshold(5);
        readinessProbe.setPeriodSeconds(180);
    }

    private final String toWatchedNamespace = "my-topic-namespace";
    private final String toImage = "my-topic-operator-image";
    private final int toReconciliationInterval = 120;

    private final List<SystemProperty> javaSystemProperties = new ArrayList<>() {{
            add(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build());
            add(new SystemPropertyBuilder().withName("something.else").withValue("42").build());
        }};

    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .withWatchedNamespace(toWatchedNamespace)
            .withImage(toImage)
            .withReconciliationIntervalSeconds(toReconciliationInterval)
            .withLivenessProbe(livenessProbe)
            .withReadinessProbe(readinessProbe)
            .withLogging(topicOperatorLogging)
            .withNewJvmOptions()
                    .withXms("128m")
                    .addAllToJavaSystemProperties(javaSystemProperties)
            .endJvmOptions()
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .withTemplate(new EntityOperatorTemplateBuilder()
                    .withNewTopicOperatorRoleBinding()
                        .withNewMetadata()
                            .withLabels(Map.of("label-1", "value-1"))
                            .withAnnotations(Map.of("anno-1", "value-1"))
                        .endMetadata()
                    .endTopicOperatorRoleBinding()
                    .build())
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
        new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(toWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(toReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_SECURITY_PROTOCOL).withValue(EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_JAVA_OPTS).withValue("-Xms128m").build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue("-Djavax.net.debug=verbose -Dsomething.else=42").build());
        return expected;
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(entityTopicOperator.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(entityTopicOperator.namespace, is(namespace));
        assertThat(entityTopicOperator.cluster, is(cluster));
        assertThat(entityTopicOperator.image, is(toImage));
        assertThat(entityTopicOperator.readinessProbeOptions.getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
        assertThat(entityTopicOperator.readinessProbeOptions.getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
        assertThat(entityTopicOperator.readinessProbeOptions.getSuccessThreshold(), is(readinessProbe.getSuccessThreshold()));
        assertThat(entityTopicOperator.readinessProbeOptions.getFailureThreshold(), is(readinessProbe.getFailureThreshold()));
        assertThat(entityTopicOperator.readinessProbeOptions.getPeriodSeconds(), is(readinessProbe.getPeriodSeconds()));
        assertThat(entityTopicOperator.livenessProbeOptions.getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
        assertThat(entityTopicOperator.livenessProbeOptions.getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
        assertThat(entityTopicOperator.livenessProbeOptions.getSuccessThreshold(), is(livenessProbe.getSuccessThreshold()));
        assertThat(entityTopicOperator.livenessProbeOptions.getFailureThreshold(), is(livenessProbe.getFailureThreshold()));
        assertThat(entityTopicOperator.livenessProbeOptions.getPeriodSeconds(), is(livenessProbe.getPeriodSeconds()));
        assertThat(entityTopicOperator.watchedNamespace(), is(toWatchedNamespace));
        assertThat(entityTopicOperator.reconciliationIntervalMs, is(toReconciliationInterval * 1000));
        assertThat(entityTopicOperator.kafkaBootstrapServers, is(KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        assertThat(entityTopicOperator.resourceLabels, is(ModelUtils.defaultResourceLabels(cluster)));
        assertThat(entityTopicOperator.logging().getLogging().getType(), is(topicOperatorLogging.getType()));
        assertThat(((InlineLogging) entityTopicOperator.logging().getLogging()).getLoggers(), is(topicOperatorLogging.getLoggers()));
    }

    @ParallelTest
    public void testFromCrdDefault() {
        EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withTopicOperator(entityTopicOperatorSpec)
                .build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityTopicOperator.watchedNamespace(), is(namespace));
        assertThat(entityTopicOperator.getImage(), is("quay.io/strimzi/operator:latest"));
        assertThat(entityTopicOperator.reconciliationIntervalMs, is(EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000));
        assertThat(entityTopicOperator.kafkaBootstrapServers, is(KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        assertThat(entityTopicOperator.resourceLabels, is(ModelUtils.defaultResourceLabels(cluster)));
        assertThat(entityTopicOperator.readinessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityTopicOperator.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityTopicOperator.livenessProbeOptions.getInitialDelaySeconds(), is(10));
        assertThat(entityTopicOperator.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(entityTopicOperator.logging().getLogging(), is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoEntityOperator() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout);
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);
        assertThat(entityTopicOperator, is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoTopicOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);
        assertThat(entityTopicOperator, is(nullValue()));
    }

    @ParallelTest
    public void testNoWatchedNamespace() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();

        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityTopicOperator.watchedNamespace(), is(namespace));
    }

    @ParallelTest
    public void testWatchedNamespace() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                    .withWatchedNamespace("some-other-namespace")
                .endTopicOperator()
                .build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .withEntityOperator(entityOperatorSpec)
                .endSpec()
                .build();

        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        assertThat(entityTopicOperator.watchedNamespace(), is("some-other-namespace"));
    }

    @ParallelTest
    public void testGetContainers() {
        Container container = entityTopicOperator.createContainer(null);
        assertThat(container.getName(), is(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME));
        assertThat(container.getImage(), is(entityTopicOperator.getImage()));
        assertThat(container.getEnv(), is(getExpectedEnvVars()));
        assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
        assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
        assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
        assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
        assertThat(container.getPorts().size(), is(1));
        assertThat(container.getPorts().get(0).getContainerPort(), is(EntityTopicOperator.HEALTHCHECK_PORT));
        assertThat(container.getPorts().get(0).getName(), is(EntityTopicOperator.HEALTHCHECK_PORT_NAME));
        assertThat(container.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(map(
                EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
                "entity-topic-operator-metrics-and-logging", "/opt/topic-operator/custom-config/",
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.ETO_CERTS_VOLUME_NAME, EntityOperator.ETO_CERTS_VOLUME_MOUNT)));
    }

    @ParallelTest
    public void testRoleBindingInOtherNamespace()   {
        RoleBinding binding = entityTopicOperator.generateRoleBindingForRole(namespace, toWatchedNamespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(toWatchedNamespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(0));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("foo-entity-operator"));
    }

    @ParallelTest
    public void testRoleBindingInTheSameNamespace()   {
        RoleBinding binding = entityTopicOperator.generateRoleBindingForRole(namespace, namespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(binding.getMetadata().getLabels().get("label-1"), is("value-1"));
        assertThat(binding.getMetadata().getAnnotations().get("anno-1"), is("value-1"));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("foo-entity-operator"));
    }

    @ParallelTest
    public void testSetupWithCruiseControlEnabled() {
        EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .build();
        Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka().withRack(new RackBuilder()
                        .withTopologyKey("foo")
                        .build())
                    .endKafka()
                    .withEntityOperator(entityOperatorSpec)
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), 
                resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        List<EnvVar> expectedEnvVars = new ArrayList<>();
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(namespace).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(toReconciliationInterval * 1000)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_SECURITY_PROTOCOL).withValue(EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_RACK_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_HOSTNAME).withValue(String.format("%s-cruise-control.%s.svc", cluster, namespace)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_PORT).withValue(String.valueOf(CRUISE_CONTROL_API_PORT)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_SSL_ENABLED).withValue(Boolean.toString(true)).build());
        expectedEnvVars.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_CRUISE_CONTROL_AUTH_ENABLED).withValue(Boolean.toString(true)).build());
        assertThat(entityTopicOperator.getEnvVars(), is(expectedEnvVars));

        Container container = entityTopicOperator.createContainer(null);
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(map(
            EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
            "entity-topic-operator-metrics-and-logging", "/opt/topic-operator/custom-config/",
            EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
            EntityOperator.ETO_CERTS_VOLUME_NAME, EntityOperator.ETO_CERTS_VOLUME_MOUNT,
            EntityOperator.ETO_CC_API_VOLUME_NAME, EntityOperator.ETO_CC_API_VOLUME_MOUNT
        )));
    }

    @ParallelTest
    public void testGenerateCruiseControlApiSecret() {
        EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .build();
        Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(
            new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(),
                resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER);

        var newSecret = entityTopicOperator.generateCruiseControlApiSecret(null);
        assertThat(newSecret, is(notNullValue()));
        assertThat(newSecret.getData(), is(notNullValue()));
        assertThat(newSecret.getData().size(), is(2));
        assertThat(newSecret.getData().get(API_TO_ADMIN_NAME_KEY), is(Util.encodeToBase64(API_TO_ADMIN_NAME)));
        assertThat(newSecret.getData().get(API_TO_ADMIN_NAME_KEY), is(notNullValue()));
        
        var name = Util.encodeToBase64(API_TO_ADMIN_NAME);
        var password = Util.encodeToBase64("changeit");
        var oldSecret = entityTopicOperator.generateCruiseControlApiSecret(
            new SecretBuilder().withData(Map.of(API_TO_ADMIN_NAME_KEY, name, API_TO_ADMIN_PASSWORD_KEY, password)).build());
        assertThat(oldSecret, is(notNullValue()));
        assertThat(oldSecret.getData(), is(notNullValue()));
        assertThat(oldSecret.getData().size(), is(2));
        assertThat(oldSecret.getData().get(API_TO_ADMIN_NAME_KEY), is(name));
        assertThat(oldSecret.getData().get(API_TO_ADMIN_PASSWORD_KEY), is(password));
        
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(
            new SecretBuilder().withData(Map.of(API_TO_ADMIN_NAME_KEY, name)).build()));
        
        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(
            new SecretBuilder().withData(Map.of(API_TO_ADMIN_PASSWORD_KEY, password)).build()));

        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(
            new SecretBuilder().withData(Map.of()).build()));

        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(
            new SecretBuilder().withData(Map.of(API_TO_ADMIN_NAME_KEY, " ", API_TO_ADMIN_PASSWORD_KEY, password)).build()));

        assertThrows(RuntimeException.class, () -> entityTopicOperator.generateCruiseControlApiSecret(
            new SecretBuilder().withData(Map.of(API_TO_ADMIN_NAME_KEY, name, API_TO_ADMIN_PASSWORD_KEY, " ")).build()));
    }
}

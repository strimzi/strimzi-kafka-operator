/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.strimzi.test.TestUtils.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EntityTopicOperatorTest {

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

    private final String toWatchedNamespace = "my-topic-namespace";
    private final String toImage = "my-topic-operator-image";
    private final int toReconciliationInterval = 90;
    private final int toZookeeperSessionTimeout = 20;
    private final int toTopicMetadataMaxAttempts = 3;

    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .withWatchedNamespace(toWatchedNamespace)
            .withImage(toImage)
            .withReconciliationIntervalSeconds(toReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(toZookeeperSessionTimeout)
            .withTopicMetadataMaxAttempts(toTopicMetadataMaxAttempts)
            .withLogging(topicOperatorLogging)
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(resource);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(EntityTopicOperator.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(toWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(toReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(toZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS).withValue(String.valueOf(toTopicMetadataMaxAttempts)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(EntityTopicOperator.DEFAULT_STRIMZI_GC_LOG_ENABED).build());
        return expected;
    }

    @Test
    public void testEnvVars()   {
        Assert.assertEquals(getExpectedEnvVars(), entityTopicOperator.getEnvVars());
    }

    @Test
    public void testFromCrd() {
        assertEquals(namespace, entityTopicOperator.namespace);
        assertEquals(cluster, entityTopicOperator.cluster);
        assertEquals(toImage, entityTopicOperator.image);
        assertEquals(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY, entityTopicOperator.readinessInitialDelay);
        assertEquals(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT, entityTopicOperator.readinessTimeout);
        assertEquals(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY, entityTopicOperator.livenessInitialDelay);
        assertEquals(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT, entityTopicOperator.livenessTimeout);
        assertEquals(toWatchedNamespace, entityTopicOperator.getWatchedNamespace());
        assertEquals(toReconciliationInterval * 1000, entityTopicOperator.getReconciliationIntervalMs());
        assertEquals(toZookeeperSessionTimeout * 1000, entityTopicOperator.getZookeeperSessionTimeoutMs());
        assertEquals(EntityTopicOperator.defaultZookeeperConnect(cluster), entityTopicOperator.getZookeeperConnect());
        assertEquals(EntityTopicOperator.defaultBootstrapServers(cluster), entityTopicOperator.getKafkaBootstrapServers());
        assertEquals(ModelUtils.defaultResourceLabels(cluster), entityTopicOperator.getResourceLabels());
        assertEquals(toTopicMetadataMaxAttempts, entityTopicOperator.getTopicMetadataMaxAttempts());
        assertEquals(topicOperatorLogging.getType(), entityTopicOperator.getLogging().getType());
        assertEquals(topicOperatorLogging.getLoggers(), ((InlineLogging) entityTopicOperator.getLogging()).getLoggers());
    }

    @Test
    public void testFromCrdDefault() {
        EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withTopicOperator(entityTopicOperatorSpec)
                .build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(resource);

        assertEquals(namespace, entityTopicOperator.getWatchedNamespace());
        assertEquals("strimzi/operator:latest", entityTopicOperator.getImage());
        assertEquals(EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000, entityTopicOperator.getReconciliationIntervalMs());
        assertEquals(EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000, entityTopicOperator.getZookeeperSessionTimeoutMs());
        assertEquals(EntityTopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS, entityTopicOperator.getTopicMetadataMaxAttempts());
        assertEquals(EntityTopicOperator.defaultZookeeperConnect(cluster), entityTopicOperator.getZookeeperConnect());
        assertEquals(EntityTopicOperator.defaultBootstrapServers(cluster), entityTopicOperator.getKafkaBootstrapServers());
        assertEquals(ModelUtils.defaultResourceLabels(cluster), entityTopicOperator.getResourceLabels());
        assertNull(entityTopicOperator.getLogging());
    }

    @Test
    public void testFromCrdNoEntityOperator() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout);
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(resource);
        assertNull(entityTopicOperator);
    }

    @Test
    public void testFromCrdNoTopicOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(resource);
        assertNull(entityTopicOperator);
    }

    @Test
    public void testGetContainers() {
        List<Container> containers = entityTopicOperator.getContainers(null);
        assertEquals(1, containers.size());

        Container container = containers.get(0);
        assertEquals(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME, container.getName());
        assertEquals(entityTopicOperator.getImage(), container.getImage());
        assertEquals(getExpectedEnvVars(), container.getEnv());
        assertEquals(new Integer(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY), container.getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT), container.getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY), container.getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT), container.getReadinessProbe().getTimeoutSeconds());
        assertEquals(1, container.getPorts().size());
        assertEquals(new Integer(EntityTopicOperator.HEALTHCHECK_PORT), container.getPorts().get(0).getContainerPort());
        assertEquals(EntityTopicOperator.HEALTHCHECK_PORT_NAME, container.getPorts().get(0).getName());
        assertEquals("TCP", container.getPorts().get(0).getProtocol());
        assertEquals(map("entity-topic-operator-metrics-and-logging", "/opt/topic-operator/custom-config/",
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                EntityOperatorTest.volumeMounts(container.getVolumeMounts()));
    }

    @Test
    public void testRoleBinding()   {
        KubernetesRoleBinding binding = entityTopicOperator.generateRoleBinding(namespace, toWatchedNamespace);

        assertEquals(namespace, binding.getSubjects().get(0).getNamespace());
        assertEquals(toWatchedNamespace, binding.getMetadata().getNamespace());
    }
}

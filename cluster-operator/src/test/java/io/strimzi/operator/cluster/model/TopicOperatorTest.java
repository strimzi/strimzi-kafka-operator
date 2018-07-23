/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TopicOperatorBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.assembly.MockCertManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TopicOperatorTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> kafkaConfig = singletonMap("foo", "bar");
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");
    private final Storage storage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private final InlineLogging topicOperatorLogging = new InlineLogging();

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
        topicOperatorLogging.setLoggers(Collections.singletonMap("topic-operator.root.logger", "OFF"));
    }

    private final String tcWatchedNamespace = "my-topic-namespace";
    private final String tcImage = "my-topic-operator-image";
    private final int tcReconciliationInterval = 90;
    private final int tcZookeeperSessionTimeout = 20;
    private final int tcTopicMetadataMaxAttempts = 3;

    private final io.strimzi.api.kafka.model.TopicOperator topicOperator = new TopicOperatorBuilder()
            .withWatchedNamespace(tcWatchedNamespace)
            .withImage(tcImage)
            .withReconciliationIntervalSeconds(tcReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(tcZookeeperSessionTimeout)
            .withTopicMetadataMaxAttempts(tcTopicMetadataMaxAttempts)
            .withLogging(topicOperatorLogging)
            .build();


    private final CertManager certManager = new MockCertManager();
    private final KafkaAssembly resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig, storage, topicOperator, kafkaLogJson, zooLogJson);
    private final TopicOperator tc = TopicOperator.fromCrd(certManager, resource, ResourceUtils.createKafkaClusterInitialSecrets(namespace, cluster));

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(TopicOperator.defaultTopicConfigMapLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(TopicOperator.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", io.strimzi.api.kafka.model.TopicOperator.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(tcWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(tcReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(tcZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS).withValue(String.valueOf(tcTopicMetadataMaxAttempts)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());

        return expected;
    }

    @Test
    public void testFromConfigMapNoConfig() {
        KafkaAssembly resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, null, kafkaLogJson, zooLogJson);
        TopicOperator tc = TopicOperator.fromCrd(certManager, resource, ResourceUtils.createKafkaClusterInitialSecrets(namespace, cluster));
        assertNull(tc);
    }

    @Test
    public void testFromConfigMapDefaultConfig() {
        KafkaAssembly resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                storage, new io.strimzi.api.kafka.model.TopicOperator(), kafkaLogJson, zooLogJson);
        TopicOperator tc = TopicOperator.fromCrd(certManager, resource, ResourceUtils.createKafkaClusterInitialSecrets(namespace, cluster));
        Assert.assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_IMAGE, tc.getImage());
        assertEquals(namespace, tc.getWatchedNamespace());
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000, tc.getReconciliationIntervalMs());
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000, tc.getZookeeperSessionTimeoutMs());
        Assert.assertEquals(TopicOperator.defaultBootstrapServers(cluster), tc.getKafkaBootstrapServers());
        Assert.assertEquals(TopicOperator.defaultZookeeperConnect(cluster), tc.getZookeeperConnect());
        Assert.assertEquals(TopicOperator.defaultTopicConfigMapLabels(cluster), tc.getTopicConfigMapLabels());
        Assert.assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS, tc.getTopicMetadataMaxAttempts());
        assertNull(tc.getLogging());
    }

    @Test
    public void testFromConfigMap() {

        Assert.assertEquals(namespace, tc.namespace);
        Assert.assertEquals(cluster, tc.cluster);
        assertEquals(tcImage, tc.image);
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_REPLICAS, tc.replicas);
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_DELAY, tc.readinessInitialDelay);
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT, tc.readinessTimeout);
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_DELAY, tc.livenessInitialDelay);
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT, tc.livenessTimeout);
        assertEquals(tcImage, tc.getImage());
        assertEquals(tcWatchedNamespace, tc.getWatchedNamespace());
        Assert.assertEquals(tcReconciliationInterval * 1000, tc.getReconciliationIntervalMs());
        Assert.assertEquals(tcZookeeperSessionTimeout * 1000, tc.getZookeeperSessionTimeoutMs());
        Assert.assertEquals(TopicOperator.defaultBootstrapServers(cluster), tc.getKafkaBootstrapServers());
        Assert.assertEquals(TopicOperator.defaultZookeeperConnect(cluster), tc.getZookeeperConnect());
        Assert.assertEquals(TopicOperator.defaultTopicConfigMapLabels(cluster), tc.getTopicConfigMapLabels());
        assertEquals(tcTopicMetadataMaxAttempts, tc.getTopicMetadataMaxAttempts());
        assertEquals(topicOperatorLogging.getType(), tc.getLogging().getType());
        assertEquals(topicOperatorLogging.getLoggers(), ((InlineLogging) tc.getLogging()).getLoggers());
    }

    @Test
    public void testGenerateDeployment() {

        Deployment dep = tc.generateDeployment();

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(2, containers.size());

        Assert.assertEquals(tc.topicOperatorName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(new Integer(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_REPLICAS), dep.getSpec().getReplicas());
        Assert.assertEquals(TopicOperator.TOPIC_OPERATOR_NAME, containers.get(0).getName());

        // checks on the main Topic Operator container
        assertEquals(tc.image, containers.get(0).getImage());
        assertEquals(getExpectedEnvVars(), containers.get(0).getEnv());
        assertEquals(new Integer(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_DELAY), containers.get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT), containers.get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_DELAY), containers.get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT), containers.get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(1, containers.get(0).getPorts().size());
        assertEquals(new Integer(TopicOperator.HEALTHCHECK_PORT), containers.get(0).getPorts().get(0).getContainerPort());
        assertEquals(TopicOperator.HEALTHCHECK_PORT_NAME, containers.get(0).getPorts().get(0).getName());
        assertEquals("TCP", containers.get(0).getPorts().get(0).getProtocol());
        assertEquals("Recreate", dep.getSpec().getStrategy().getType());
        assertEquals(TopicOperator.TLS_SIDECAR_VOLUME_NAME, containers.get(0).getVolumeMounts().get(1).getName());
        assertEquals(TopicOperator.TLS_SIDECAR_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertLoggingConfig(dep);
        // checks on the TLS sidecar container
        assertEquals(io.strimzi.api.kafka.model.TopicOperator.DEFAULT_TLS_SIDECAR_IMAGE, containers.get(1).getImage());
        assertEquals(TopicOperator.defaultZookeeperConnect(cluster), AbstractModel.containerEnvVars(containers.get(1)).get(TopicOperator.ENV_VAR_ZOOKEEPER_CONNECT));
        assertEquals(TopicOperator.TLS_SIDECAR_VOLUME_NAME, containers.get(1).getVolumeMounts().get(0).getName());
        assertEquals(TopicOperator.TLS_SIDECAR_VOLUME_MOUNT, containers.get(1).getVolumeMounts().get(0).getMountPath());
    }

    @Test
    public void testEnvVars()   {
        Assert.assertEquals(getExpectedEnvVars(), tc.getEnvVars());
    }

    @Rule
    public ResourceTester<KafkaAssembly, TopicOperator> helper = new ResourceTester<>(KafkaAssembly.class, TopicOperator::fromCrd);

    @Test
    public void withAffinity() throws IOException {
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment().getSpec().getTemplate().getSpec().getAffinity());
    }

    private void assertLoggingConfig(Deployment dep) {
        Volume volume = dep.getSpec().getTemplate().getSpec().getVolumes().get(0);
        VolumeMount volumeMount = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0);

        assertEquals("/opt/topic-operator/custom-config/", volumeMount.getMountPath());
        assertEquals("topic-operator-metrics-and-logging", volumeMount.getName());
        assertEquals("topic-operator-metrics-and-logging", volume.getName());
        assertEquals("foo-topic-operator-config", volume.getConfigMap().getName());
    }
}

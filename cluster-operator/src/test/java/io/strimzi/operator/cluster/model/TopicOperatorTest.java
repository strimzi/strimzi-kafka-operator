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
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.SingleVolumeStorage;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.TopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.operator.cluster.ResourceUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TopicOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            singletonMap("2.0.0", "strimzi/kafka:latest-kafka-2.0.0"),
            emptyMap(), emptyMap(), emptyMap()) { };

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> kafkaConfig = singletonMap("foo", "bar");
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");
    private final Storage kafkaStorage = new EphemeralStorage();
    private final SingleVolumeStorage zkStorage = new EphemeralStorage();
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

    private final TlsSidecar tlsSidecar = new TlsSidecarBuilder()
            .withLivenessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .withReadinessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .build();

    private final TopicOperatorSpec topicOperator = new TopicOperatorSpecBuilder()
            .withWatchedNamespace(tcWatchedNamespace)
            .withImage(tcImage)
            .withReconciliationIntervalSeconds(tcReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(tcZookeeperSessionTimeout)
            .withTopicMetadataMaxAttempts(tcTopicMetadataMaxAttempts)
            .withLogging(topicOperatorLogging)
            .withTlsSidecar(tlsSidecar)
            .build();

    private final Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig, kafkaStorage, zkStorage, topicOperator, kafkaLogJson, zooLogJson);
    private final TopicOperator tc = TopicOperator.fromCrd(resource, VERSIONS);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(TopicOperator.defaultTopicConfigMapLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(TopicOperator.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", TopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(tcWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(tcReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(tcZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS).withValue(String.valueOf(tcTopicMetadataMaxAttempts)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(TopicOperator.DEFAULT_STRIMZI_GC_LOG_ENABED).build());

        return expected;
    }

    @Test
    public void testFromConfigMapNoConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, null, kafkaLogJson, zooLogJson);
        TopicOperator tc = TopicOperator.fromCrd(resource, VERSIONS);
        assertNull(tc);
    }

    @Test
    public void testFromConfigMapDefaultConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, new TopicOperatorSpec(), kafkaLogJson, zooLogJson);
        TopicOperator tc = TopicOperator.fromCrd(resource, VERSIONS);
        Assert.assertEquals("strimzi/operator:latest", tc.getImage());
        assertEquals(namespace, tc.getWatchedNamespace());
        assertEquals(TopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000, tc.getReconciliationIntervalMs());
        assertEquals(TopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000, tc.getZookeeperSessionTimeoutMs());
        Assert.assertEquals(TopicOperator.defaultBootstrapServers(cluster), tc.getKafkaBootstrapServers());
        Assert.assertEquals(TopicOperator.defaultZookeeperConnect(cluster), tc.getZookeeperConnect());
        Assert.assertEquals(TopicOperator.defaultTopicConfigMapLabels(cluster), tc.getTopicConfigMapLabels());
        Assert.assertEquals(TopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS, tc.getTopicMetadataMaxAttempts());
        assertNull(tc.getLogging());
    }

    @Test
    public void testFromConfigMap() {

        Assert.assertEquals(namespace, tc.namespace);
        Assert.assertEquals(cluster, tc.cluster);
        assertEquals(tcImage, tc.image);
        assertEquals(TopicOperatorSpec.DEFAULT_REPLICAS, tc.replicas);
        assertEquals(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY, tc.readinessInitialDelay);
        assertEquals(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT, tc.readinessTimeout);
        assertEquals(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY, tc.livenessInitialDelay);
        assertEquals(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT, tc.livenessTimeout);
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

        Deployment dep = tc.generateDeployment(true, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(2, containers.size());

        Assert.assertEquals(tc.topicOperatorName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(new Integer(TopicOperatorSpec.DEFAULT_REPLICAS), dep.getSpec().getReplicas());
        Assert.assertEquals(TopicOperator.TOPIC_OPERATOR_NAME, containers.get(0).getName());
        assertEquals(1, dep.getMetadata().getOwnerReferences().size());
        assertEquals(tc.createOwnerReference(), dep.getMetadata().getOwnerReferences().get(0));

        // checks on the main Topic Operator container
        assertEquals(tc.image, containers.get(0).getImage());
        assertEquals(getExpectedEnvVars(), containers.get(0).getEnv());
        assertEquals(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY), containers.get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT), containers.get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY), containers.get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT), containers.get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(1, containers.get(0).getPorts().size());
        assertEquals(new Integer(TopicOperator.HEALTHCHECK_PORT), containers.get(0).getPorts().get(0).getContainerPort());
        assertEquals(TopicOperator.HEALTHCHECK_PORT_NAME, containers.get(0).getPorts().get(0).getName());
        assertEquals("TCP", containers.get(0).getPorts().get(0).getProtocol());
        assertEquals("Recreate", dep.getSpec().getStrategy().getType());
        assertEquals(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, containers.get(0).getVolumeMounts().get(1).getName());
        assertEquals(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertLoggingConfig(dep);
        // checks on the TLS sidecar container
        Container tlsSidecarContainer = containers.get(1);
        assertEquals(image, tlsSidecarContainer.getImage());
        assertEquals(TopicOperator.defaultZookeeperConnect(cluster), AbstractModel.containerEnvVars(tlsSidecarContainer).get(TopicOperator.ENV_VAR_ZOOKEEPER_CONNECT));
        assertEquals(TlsSidecarLogLevel.NOTICE.toValue(), AbstractModel.containerEnvVars(tlsSidecarContainer).get(ModelUtils.TLS_SIDECAR_LOG_LEVEL));
        assertEquals(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, tlsSidecarContainer.getVolumeMounts().get(0).getName());
        assertEquals(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT, tlsSidecarContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals(new Integer(tlsHealthDelay), tlsSidecarContainer.getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(tlsHealthTimeout), tlsSidecarContainer.getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(tlsHealthDelay), tlsSidecarContainer.getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(tlsHealthTimeout), tlsSidecarContainer.getLivenessProbe().getTimeoutSeconds());
    }

    @Test
    public void testEnvVars()   {
        Assert.assertEquals(getExpectedEnvVars(), tc.getEnvVars());
    }

    @Rule
    public ResourceTester<Kafka, TopicOperator> helper = new ResourceTester<>(Kafka.class, VERSIONS, TopicOperator::fromCrd);

    @Test
    public void withAffinity() throws IOException {
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment(true, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    private void assertLoggingConfig(Deployment dep) {
        Volume volume = dep.getSpec().getTemplate().getSpec().getVolumes().get(0);
        VolumeMount volumeMount = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0);

        assertEquals("/opt/topic-operator/custom-config/", volumeMount.getMountPath());
        assertEquals("topic-operator-metrics-and-logging", volumeMount.getName());
        assertEquals("topic-operator-metrics-and-logging", volume.getName());
        assertEquals("foo-topic-operator-config", volume.getConfigMap().getName());
    }

    @Test
    public void testImagePullPolicy() {
        Deployment dep = tc.generateDeployment(true, ImagePullPolicy.ALWAYS);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy());

        dep = tc.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT);
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy());
    }

    @Test
    public void testRoleBinding()   {
        KubernetesRoleBinding binding = tc.generateRoleBinding(namespace, tcWatchedNamespace);

        assertEquals(namespace, binding.getSubjects().get(0).getNamespace());
        assertEquals(tcWatchedNamespace, binding.getMetadata().getNamespace());
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}

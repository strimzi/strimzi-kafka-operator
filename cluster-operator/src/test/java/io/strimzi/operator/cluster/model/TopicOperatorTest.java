/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TopicOperatorTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String kafkaConfigJson = "{\"foo\":\"bar\"}";
    private final String zooConfigJson = "{\"foo\":\"bar\"}";
    private final String storageJson = "{\"type\": \"ephemeral\"}";

    private final String tcWatchedNamespace = "my-topic-namespace";
    private final String tcImage = "my-topic-operator-image";
    private final String tcReconciliationInterval = "900000";
    private final String tcZookeeperSessionTimeout = "20000";
    private final int tcTopicMetadataMaxAttempts = 3;

    private final String topicOperatorJson = "{ " +
            "\"watchedNamespace\":\"" + tcWatchedNamespace + "\", " +
            "\"image\":\"" + tcImage + "\", " +
            "\"reconciliationInterval\":\"" + tcReconciliationInterval + "\", " +
            "\"zookeeperSessionTimeout\":\"" + tcZookeeperSessionTimeout + "\"," +
            "\"topicMetadataMaxAttempts\":" + tcTopicMetadataMaxAttempts +
            " }";

    private final ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, kafkaConfigJson, zooConfigJson, storageJson, topicOperatorJson, null);
    private final TopicOperator tc = TopicOperator.fromConfigMap(cm);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_CONFIGMAP_LABELS).withValue(TopicOperator.defaultTopicConfigMapLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_KAFKA_BOOTSTRAP_SERVERS).withValue(TopicOperator.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_ZOOKEEPER_CONNECT).withValue(TopicOperator.defaultZookeeperConnect(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_WATCHED_NAMESPACE).withValue(tcWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(tcReconciliationInterval)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(tcZookeeperSessionTimeout)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.KEY_TOPIC_METADATA_MAX_ATTEMPTS).withValue(String.valueOf(tcTopicMetadataMaxAttempts)).build());

        return expected;
    }

    @Test
    public void testFromConfigMapNoConfig() {

        ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson);
        TopicOperator tc = TopicOperator.fromConfigMap(cm);

        assertNull(tc);
    }

    @Test
    public void testFromConfigMapDefaultConfig() {

        ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, kafkaConfigJson, zooConfigJson, storageJson, "{ }", null);
        TopicOperator tc = TopicOperator.fromConfigMap(cm);

        assertEquals(TopicOperator.DEFAULT_IMAGE, tc.getImage());
        assertEquals(namespace, tc.getWatchedNamespace());
        assertEquals(TopicOperator.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, tc.getReconciliationIntervalMs());
        assertEquals(TopicOperator.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS, tc.getZookeeperSessionTimeoutMs());
        assertEquals(TopicOperator.defaultBootstrapServers(cluster), tc.getKafkaBootstrapServers());
        assertEquals(TopicOperator.defaultZookeeperConnect(cluster), tc.getZookeeperConnect());
        assertEquals(TopicOperator.defaultTopicConfigMapLabels(cluster), tc.getTopicConfigMapLabels());
        assertEquals(TopicOperator.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS, tc.getTopicMetadataMaxAttempts());
    }

    @Test
    public void testFromConfigMap() {

        assertEquals(namespace, tc.namespace);
        assertEquals(cluster, tc.cluster);
        assertEquals(tcImage, tc.image);
        assertEquals(TopicOperator.DEFAULT_REPLICAS, tc.replicas);
        assertEquals(TopicOperator.DEFAULT_HEALTHCHECK_DELAY, tc.healthCheckInitialDelay);
        assertEquals(TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT, tc.healthCheckTimeout);
        assertEquals(tcImage, tc.getImage());
        assertEquals(tcWatchedNamespace, tc.getWatchedNamespace());
        assertEquals(tcReconciliationInterval, tc.getReconciliationIntervalMs());
        assertEquals(tcZookeeperSessionTimeout, tc.getZookeeperSessionTimeoutMs());
        assertEquals(TopicOperator.defaultBootstrapServers(cluster), tc.getKafkaBootstrapServers());
        assertEquals(TopicOperator.defaultZookeeperConnect(cluster), tc.getZookeeperConnect());
        assertEquals(TopicOperator.defaultTopicConfigMapLabels(cluster), tc.getTopicConfigMapLabels());
        assertEquals(tcTopicMetadataMaxAttempts, tc.getTopicMetadataMaxAttempts());
    }

    @Test
    public void testFromDeployment() {

        TopicOperator tcFromDep = TopicOperator.fromAssembly(namespace, cluster, tc.generateDeployment());

        assertEquals(tc.namespace, tcFromDep.namespace);
        assertEquals(tc.cluster, tcFromDep.cluster);
        assertEquals(tc.image, tcFromDep.image);
        assertEquals(tc.replicas, tcFromDep.replicas);
        assertEquals(tc.healthCheckInitialDelay, tcFromDep.healthCheckInitialDelay);
        assertEquals(tc.healthCheckTimeout, tcFromDep.healthCheckTimeout);
        assertEquals(tc.getImage(), tcFromDep.getImage());
        assertEquals(tc.getWatchedNamespace(), tcFromDep.getWatchedNamespace());
        assertEquals(tc.getReconciliationIntervalMs(), tcFromDep.getReconciliationIntervalMs());
        assertEquals(tc.getZookeeperSessionTimeoutMs(), tcFromDep.getZookeeperSessionTimeoutMs());
        assertEquals(tc.getKafkaBootstrapServers(), tcFromDep.getKafkaBootstrapServers());
        assertEquals(tc.getZookeeperConnect(), tcFromDep.getZookeeperConnect());
        assertEquals(tc.getTopicConfigMapLabels(), tcFromDep.getTopicConfigMapLabels());
        assertEquals(tc.getTopicMetadataMaxAttempts(), tcFromDep.getTopicMetadataMaxAttempts());
    }

    @Test
    public void testGenerateDeployment() {

        Deployment dep = tc.generateDeployment();

        assertEquals(tc.topicOperatorName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(new Integer(TopicOperator.DEFAULT_REPLICAS), dep.getSpec().getReplicas());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(tc.topicOperatorName(cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(tc.image, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(new Integer(TopicOperator.DEFAULT_HEALTHCHECK_DELAY), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(TopicOperator.DEFAULT_HEALTHCHECK_DELAY), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        assertEquals(new Integer(TopicOperator.HEALTHCHECK_PORT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort());
        assertEquals(TopicOperator.HEALTHCHECK_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());
        assertEquals("Recreate", dep.getSpec().getStrategy().getType());
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), tc.getEnvVars());
    }

    @Rule
    public ResourceTester<TopicOperator> helper = new ResourceTester<>(TopicOperator::fromConfigMap);

    @Test
    public void withAffinity() throws IOException {
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment().getSpec().getTemplate().getSpec().getAffinity());
    }

}

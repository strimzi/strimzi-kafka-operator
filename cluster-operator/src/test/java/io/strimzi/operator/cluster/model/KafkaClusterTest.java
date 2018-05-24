/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTerm;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.Test;

import java.util.List;

import static io.strimzi.operator.cluster.ResourceUtils.labels;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KafkaClusterTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String configurationJson = "{\"foo\":\"bar\"}";
    private final ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson);
    private final KafkaCluster kc = KafkaCluster.fromConfigMap(cm);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsConfigMap();
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.METRICS_CONFIG_FILE));
    }

    @Test
    public void testGenerateService() {
        Service headful = kc.generateService();
        checkService(headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster)), headful.getSpec().getSelector());
        assertEquals(2, headful.getSpec().getPorts().size());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headful.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headful.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headful.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headful.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = kc.generateHeadlessService();
        checkHeadlessService(headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(KafkaCluster.headlessName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster)), headless.getSpec().getSelector());
        assertEquals(2, headless.getSpec().getPorts().size());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headless.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headless.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headless.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headless.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss, cm);
    }

    @Test
    public void testGenerateStatefulSetWithRack() {
        ConfigMap cm =
                ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                        metricsCmJson, configurationJson, "{}", "{\"type\": \"ephemeral\"}",
                        null, "{\"topologyKey\": \"rack-key\"}");
        KafkaCluster kc = KafkaCluster.fromConfigMap(cm);
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss, cm);
    }

    private void checkStatefulSet(StatefulSet ss, ConfigMap cm) {
        assertEquals(KafkaCluster.kafkaClusterName(cluster), ss.getMetadata().getName());
        // ... in the same namespace ...
        assertEquals(namespace, ss.getMetadata().getNamespace());
        // ... with these labels
        assertEquals(labels("strimzi.io/cluster", cluster,
                "strimzi.io/type", "kafka",
                "my-user-label", "cromulent",
                "strimzi.io/name", KafkaCluster.kafkaClusterName(cluster)),
                ss.getMetadata().getLabels());

        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image, ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(new Integer(healthTimeout), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals("foo=bar\n", AbstractModel.containerEnvVars(ss.getSpec().getTemplate().getSpec().getContainers().get(0)).get(KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION));

        if (cm.getData().get("kafka-rack") != null) {

            RackConfig rackConfig = RackConfig.fromJson(cm.getData().get("kafka-rack"));

            // check that the pod spec contains anti-affinity rules with the right topology key
            PodSpec podSpec = ss.getSpec().getTemplate().getSpec();
            assertNotNull(podSpec.getAffinity());
            assertNotNull(podSpec.getAffinity().getPodAntiAffinity());
            assertNotNull(podSpec.getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution());
            List<WeightedPodAffinityTerm> terms = podSpec.getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution();
            assertNotNull(terms);
            assertTrue(terms.size() > 0);

            boolean isTopologyKey = false;
            for (WeightedPodAffinityTerm term: terms) {
                isTopologyKey = term.getPodAffinityTerm().getTopologyKey().equals(rackConfig.getTopologyKey());
                if (isTopologyKey)
                    break;
            }
            assertTrue(isTopologyKey);

            // check that pod spec contains the init Kafka container
            List<Container> initContainers = podSpec.getInitContainers();
            assertNotNull(initContainers);
            assertTrue(initContainers.size() > 0);

            boolean isInitKafka = false;
            for (Container container: initContainers) {
                isInitKafka = container.getName().equals(KafkaCluster.INIT_KAFKA_NAME);
                if (isInitKafka)
                    break;
            }
            assertTrue(isInitKafka);
        }
    }

    /**
     * Check that a KafkaCluster from a statefulset matches the one from a ConfigMap
     */
    @Test
    public void testClusterFromStatefulSet() {
        StatefulSet ss = kc.generateStatefulSet(true);
        KafkaCluster kc2 = KafkaCluster.fromAssembly(ss, namespace, cluster);
        // Don't check the metrics CM, since this isn't restored from the StatefulSet
        checkService(kc2.generateService());
        checkHeadlessService(kc2.generateHeadlessService());
        checkStatefulSet(kc2.generateStatefulSet(true), cm);
    }

    // TODO test volume claim templates

    @Test
    public void testPodNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(KafkaCluster.kafkaClusterName(cluster) + "-" + i, kc.getPodName(i));
        }
    }

    @Test
    public void testPvcNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(kc.VOLUME_NAME + "-" + KafkaCluster.kafkaPodName(cluster, i), kc.getPersistentVolumeClaimName(i));
        }
    }

    @Test
    public void testCorruptedConfigMap() {
        try {
            ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, "{\"key.name\": oops}");
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("key.name", e.getKey());
        }
    }

    @Test
    public void testCorruptedConfigMapMetrics() {
        try {
            ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "", configurationJson);
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("JSON - empty value", e.getKey());
        }

        try {
            ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "{\"lowercaseOutputName\" : true \n," +
                            "\"rules\": }", configurationJson);
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("Unexpected character - }", e.getKey());
        }

        try {
            ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "    {\n" +
                            "    \"lowercaseOutputName\": true,\n" +
                            "    \"rules\": [{\n" +
                            "    \"pattern\": \"kafka.server<type=(.+), name=(.+)PerSec\\\\w*><>Count\",\n" +
                            "    \"name\": \"kafka_server_$1_$2_total\"\n" +
                            "    },\n" +
                            "    {\n" +
                            "    \"pattern\": \"kafka.server<type=(.+), name=(.+)PerSec\\\\w*, topic=(.+)><>Count\",\n" +
                            "    \"name\": \"x\",\n" +
                            "    \"labels\": \n" +
                            "    }\n" +
                            "    ]\n" +
                            "    }", configurationJson);
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("Unexpected character - }", e.getKey());
        }

        try {
            ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "{\"lowercaseOutputName\" : tru \n," +
                            "\"rules\": \"I am valid\" }", configurationJson);
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("lowercaseOutputName", e.getKey());
        }
    }
}

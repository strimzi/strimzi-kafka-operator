/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.BinaryBuildSource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageChangeTrigger;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.strimzi.operator.cluster.ResourceUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaConnectS2IClusterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String configurationJson = "{\"foo\":\"bar\"}";
    private final String expectedConfiguration = "group.id=connect-cluster\n" +
            "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.key.converter.schemas.enable=false\n" +
            "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "config.storage.topic=connect-cluster-configs\n" +
            "status.storage.topic=connect-cluster-status\n" +
            "offset.storage.topic=connect-cluster-offsets\n" +
            "foo=bar\n" +
            "internal.key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.value.converter.schemas.enable=false\n" +
            "internal.value.converter=org.apache.kafka.connect.json.JsonConverter\n";
    private final String defaultConfiguration = "group.id=connect-cluster\n" +
            "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.key.converter.schemas.enable=false\n" +
            "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "config.storage.topic=connect-cluster-configs\n" +
            "status.storage.topic=connect-cluster-status\n" +
            "offset.storage.topic=connect-cluster-offsets\n" +
            "internal.key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.value.converter.schemas.enable=false\n" +
            "internal.value.converter=org.apache.kafka.connect.json.JsonConverter\n";
    private final boolean insecureSourceRepo = false;

    private final ConfigMap cm = ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
            healthDelay, healthTimeout, metricsCmJson, configurationJson, insecureSourceRepo);
    private final KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromConfigMap(cm);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsConfigMap();
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.METRICS_CONFIG_FILE));
    }

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<EnvVar>();
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION).withValue(expectedConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION).withValue("1.0").build());

        return expected;
    }

    @Test
    public void testDefaultValues() {
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(namespace, cluster));

        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", kc.image);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_REPLICAS, kc.replicas);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_IMAGE, kc.sourceImageBaseName + ":" + kc.sourceImageTag);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_DELAY, kc.healthCheckInitialDelay);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.healthCheckTimeout);
        assertEquals(defaultConfiguration, kc.getConfiguration().getConfiguration());
        assertFalse(kc.isInsecureSourceRepository());
    }

    @Test
    public void testFromConfigMap() {
        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", kc.image);
        assertEquals(replicas, kc.replicas);
        assertEquals(image, kc.sourceImageBaseName + ":" + kc.sourceImageTag);
        assertEquals(healthDelay, kc.healthCheckInitialDelay);
        assertEquals(healthTimeout, kc.healthCheckTimeout);
        assertEquals(expectedConfiguration, kc.getConfiguration().getConfiguration());
        assertFalse(kc.isInsecureSourceRepository());
    }

    @Test
    public void testFromDeployment() {
        KafkaConnectS2ICluster newKc = KafkaConnectS2ICluster.fromAssembly(namespace, cluster, kc.generateDeploymentConfig(), kc.generateSourceImageStream());

        assertEquals(newKc.kafkaConnectClusterName(cluster) + ":latest", newKc.image);
        assertEquals(replicas, newKc.replicas);
        assertEquals(image, newKc.sourceImageBaseName + ":" + newKc.sourceImageTag);
        assertEquals(healthDelay, newKc.healthCheckInitialDelay);
        assertEquals(healthTimeout, newKc.healthCheckTimeout);
        assertEquals(expectedConfiguration, kc.getConfiguration().getConfiguration());
    }

    @Test
    public void testFromDeploymentWithDefaultValues() {
        KafkaConnectS2ICluster defaultsKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(namespace, cluster));
        KafkaConnectS2ICluster newKc = KafkaConnectS2ICluster.fromAssembly(namespace, cluster, defaultsKc.generateDeploymentConfig(), defaultsKc.generateSourceImageStream());

        assertEquals(newKc.kafkaConnectClusterName(cluster) + ":latest", newKc.image);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_REPLICAS, newKc.replicas);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_IMAGE, newKc.sourceImageBaseName + ":" + newKc.sourceImageTag);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_DELAY, newKc.healthCheckInitialDelay);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_TIMEOUT, newKc.healthCheckTimeout);
        assertEquals(defaultsKc.getConfiguration().getConfiguration(), newKc.getConfiguration().getConfiguration());
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), kc.getEnvVars());
    }

    @Test
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        Map<String, String> expectedLabels = ResourceUtils.labels(
                "my-user-label", "cromulent",
                Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, svc.getMetadata().getLabels());
        assertEquals(expectedLabels, svc.getSpec().getSelector());
        assertEquals(2, svc.getSpec().getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateDeploymentConfig()   {
        DeploymentConfig dep = kc.generateDeploymentConfig();

        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(kc.kafkaConnectClusterName(this.cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(kc.kafkaConnectClusterName(this.cluster) + ":latest", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());
        assertEquals(2, dep.getSpec().getTriggers().size());
        assertEquals("ConfigChange", dep.getSpec().getTriggers().get(0).getType());
        assertEquals("ImageChange", dep.getSpec().getTriggers().get(1).getType());
        assertEquals(true, dep.getSpec().getTriggers().get(1).getImageChangeParams().getAutomatic());
        assertEquals(1, dep.getSpec().getTriggers().get(1).getImageChangeParams().getContainerNames().size());
        assertEquals(kc.kafkaConnectClusterName(this.cluster), dep.getSpec().getTriggers().get(1).getImageChangeParams().getContainerNames().get(0));
        assertEquals(kc.kafkaConnectClusterName(this.cluster) + ":latest", dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getName());
        assertEquals("ImageStreamTag", dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getKind());
        assertEquals("Rolling", dep.getSpec().getStrategy().getType());
        assertEquals(new Integer(1), dep.getSpec().getStrategy().getRollingParams().getMaxSurge().getIntVal());
        assertEquals(new Integer(0), dep.getSpec().getStrategy().getRollingParams().getMaxUnavailable().getIntVal());
    }

    @Test
    public void testGenerateBuildConfig() {
        BuildConfig bc = kc.generateBuildConfig();

        assertEquals(kc.kafkaConnectClusterName(cluster), bc.getMetadata().getName());
        assertEquals(namespace, bc.getMetadata().getNamespace());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster)), bc.getMetadata().getLabels());
        assertEquals("ImageStreamTag", bc.getSpec().getOutput().getTo().getKind());
        assertEquals(kc.image, bc.getSpec().getOutput().getTo().getName());
        assertEquals("Serial", bc.getSpec().getRunPolicy());
        assertEquals("Binary", bc.getSpec().getSource().getType());
        assertEquals(new BinaryBuildSource(), bc.getSpec().getSource().getBinary());
        assertEquals("Source", bc.getSpec().getStrategy().getType());
        assertEquals("ImageStreamTag", bc.getSpec().getStrategy().getSourceStrategy().getFrom().getKind());
        assertEquals(kc.getSourceImageStreamName() + ":" + kc.sourceImageTag, bc.getSpec().getStrategy().getSourceStrategy().getFrom().getName());
        assertEquals(2, bc.getSpec().getTriggers().size());
        assertEquals("ConfigChange", bc.getSpec().getTriggers().get(0).getType());
        assertEquals("ImageChange", bc.getSpec().getTriggers().get(1).getType());
        assertEquals(new ImageChangeTrigger(), bc.getSpec().getTriggers().get(1).getImageChange());
    }

    @Test
    public void testGenerateSourceImageStream() {
        ImageStream is = kc.generateSourceImageStream();

        assertEquals(kc.getSourceImageStreamName(), is.getMetadata().getName());
        assertEquals(namespace, is.getMetadata().getNamespace());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.getSourceImageStreamName()), is.getMetadata().getLabels());
        assertEquals(false, is.getSpec().getLookupPolicy().getLocal());
        assertEquals(1, is.getSpec().getTags().size());
        assertEquals(image.substring(image.lastIndexOf(":") + 1), is.getSpec().getTags().get(0).getName());
        assertEquals("DockerImage", is.getSpec().getTags().get(0).getFrom().getKind());
        assertEquals(image, is.getSpec().getTags().get(0).getFrom().getName());
        assertNull(is.getSpec().getTags().get(0).getImportPolicy());
        assertNull(is.getSpec().getTags().get(0).getReferencePolicy());
    }

    @Test
    public void testInsecureSourceRepo() {
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout,  metricsCmJson, configurationJson, true));

        assertTrue(kc.isInsecureSourceRepository());

        ImageStream is = kc.generateSourceImageStream();

        assertEquals(kc.getSourceImageStreamName(), is.getMetadata().getName());
        assertEquals(namespace, is.getMetadata().getNamespace());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.getSourceImageStreamName()), is.getMetadata().getLabels());
        assertEquals(false, is.getSpec().getLookupPolicy().getLocal());
        assertEquals(1, is.getSpec().getTags().size());
        assertEquals(image.substring(image.lastIndexOf(":") + 1), is.getSpec().getTags().get(0).getName());
        assertEquals("DockerImage", is.getSpec().getTags().get(0).getFrom().getKind());
        assertEquals(image, is.getSpec().getTags().get(0).getFrom().getName());
        assertTrue(is.getSpec().getTags().get(0).getImportPolicy().getInsecure());
        assertEquals("Local", is.getSpec().getTags().get(0).getReferencePolicy().getType());
    }

    @Test
    public void testGenerateTargetImageStream() {
        ImageStream is = kc.generateTargetImageStream();

        assertEquals(kc.kafkaConnectClusterName(cluster), is.getMetadata().getName());
        assertEquals(namespace, is.getMetadata().getNamespace());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster)), is.getMetadata().getLabels());
        assertEquals(true, is.getSpec().getLookupPolicy().getLocal());
    }

    @Test
    public void testCorruptedConfigMapMetrics() {
        try {
            ConfigMap cm = ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "", configurationJson, insecureSourceRepo);
            KafkaConnectS2ICluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("JSON - empty value", e.getKey());
        }

        try {
            ConfigMap cm = ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "{\"lowercaseOutputName\" : true \n," +
                            "\"rules\": }", configurationJson, insecureSourceRepo);
            KafkaConnectS2ICluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("Unexpected character - }", e.getKey());
        }

        try {
            ConfigMap cm = ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
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
                            "    }", configurationJson, insecureSourceRepo);
            KafkaConnectS2ICluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("Unexpected character - }", e.getKey());
        }

        try {
            ConfigMap cm = ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                    "{\"lowercaseOutputName\" : tru \n," +
                            "\"rules\": \"I am valid\" }", configurationJson, insecureSourceRepo);
            KafkaConnectS2ICluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("lowercaseOutputName", e.getKey());
        }
    }
}

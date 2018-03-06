/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.openshift.api.model.BinaryBuildSource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildTriggerPolicy;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageChangeTrigger;
import io.fabric8.openshift.api.model.ImageLookupPolicyBuilder;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.api.model.TagReference;
import io.strimzi.controller.cluster.ResourceUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaConnectS2IClusterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String bootstrapServers = "my-cluster-kafka:9092";
    private final String groupID = "my-cluster-group";
    private final int configReplicationFactor = 1;
    private final int offsetReplicationFactor = 1;
    private final int statusReplicationFactor = 1;
    private final String keyConverter = "org.apache.kafka.connect.json.AvroConverter";
    private final String valueConverter = "org.apache.kafka.connect.json.AvroConverter";
    private final boolean keyConverterSchemas = false;
    private final boolean valuesConverterSchema = false;

    private final ConfigMap cm = ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
            healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
            statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema);
    private final KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromConfigMap(cm);

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<EnvVar>();
        expected.add(new EnvVarBuilder().withName(kc.KEY_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_GROUP_ID).withValue(groupID).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_KEY_CONVERTER).withValue(keyConverter).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE).withValue(Boolean.toString(keyConverterSchemas)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_VALUE_CONVERTER).withValue(valueConverter).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE).withValue(Boolean.toString(valuesConverterSchema)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_CONFIG_STORAGE_REPLICATION_FACTOR).withValue(Integer.toString(configReplicationFactor)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_OFFSET_STORAGE_REPLICATION_FACTOR).withValue(Integer.toString(offsetReplicationFactor)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_STATUS_STORAGE_REPLICATION_FACTOR).withValue(Integer.toString(statusReplicationFactor)).build());

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
        assertEquals(KafkaConnectS2ICluster.DEFAULT_BOOTSTRAP_SERVERS, kc.bootstrapServers);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR, kc.configStorageReplicationFactor);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR, kc.offsetStorageReplicationFactor);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR, kc.statusStorageReplicationFactor);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_GROUP_ID, kc.groupId);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_KEY_CONVERTER, kc.keyConverter);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE, kc.keyConverterSchemasEnable);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_VALUE_CONVERTER, kc.valueConverter);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE, kc.valueConverterSchemasEnable);
    }

    @Test
    public void testFromConfigMap() {
        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", kc.image);
        assertEquals(replicas, kc.replicas);
        assertEquals(image, kc.sourceImageBaseName + ":" + kc.sourceImageTag);
        assertEquals(healthDelay, kc.healthCheckInitialDelay);
        assertEquals(healthTimeout, kc.healthCheckTimeout);
        assertEquals(bootstrapServers, kc.bootstrapServers);
        assertEquals(configReplicationFactor, kc.configStorageReplicationFactor);
        assertEquals(offsetReplicationFactor, kc.offsetStorageReplicationFactor);
        assertEquals(statusReplicationFactor, kc.statusStorageReplicationFactor);
        assertEquals(groupID, kc.groupId);
        assertEquals(keyConverter, kc.keyConverter);
        assertEquals(keyConverterSchemas, kc.keyConverterSchemasEnable);
        assertEquals(valueConverter, kc.valueConverter);
        assertEquals(valuesConverterSchema, kc.valueConverterSchemasEnable);
    }

    @Test
    public void testFromDeployment() {
        KafkaConnectS2ICluster newKc = KafkaConnectS2ICluster.fromDeployment(namespace, cluster, kc.generateDeploymentConfig(), kc.generateSourceImageStream());

        assertEquals(newKc.kafkaConnectClusterName(cluster) + ":latest", newKc.image);
        assertEquals(replicas, newKc.replicas);
        assertEquals(image, newKc.sourceImageBaseName + ":" + newKc.sourceImageTag);
        assertEquals(healthDelay, newKc.healthCheckInitialDelay);
        assertEquals(healthTimeout, newKc.healthCheckTimeout);
        assertEquals(bootstrapServers, newKc.bootstrapServers);
        assertEquals(configReplicationFactor, newKc.configStorageReplicationFactor);
        assertEquals(offsetReplicationFactor, newKc.offsetStorageReplicationFactor);
        assertEquals(statusReplicationFactor, newKc.statusStorageReplicationFactor);
        assertEquals(groupID, newKc.groupId);
        assertEquals(keyConverter, newKc.keyConverter);
        assertEquals(keyConverterSchemas, newKc.keyConverterSchemasEnable);
        assertEquals(valueConverter, newKc.valueConverter);
        assertEquals(valuesConverterSchema, newKc.valueConverterSchemasEnable);
    }

    @Test
    public void testFromDeploymentWithDefaultValues() {
        KafkaConnectS2ICluster defaultsKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(namespace, cluster));
        KafkaConnectS2ICluster newKc = KafkaConnectS2ICluster.fromDeployment(namespace, cluster, defaultsKc.generateDeploymentConfig(), defaultsKc.generateSourceImageStream());

        assertEquals(newKc.kafkaConnectClusterName(cluster) + ":latest", newKc.image);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_REPLICAS, newKc.replicas);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_IMAGE, newKc.sourceImageBaseName + ":" + newKc.sourceImageTag);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_DELAY, newKc.healthCheckInitialDelay);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_TIMEOUT, newKc.healthCheckTimeout);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_BOOTSTRAP_SERVERS, newKc.bootstrapServers);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR, newKc.configStorageReplicationFactor);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR, newKc.offsetStorageReplicationFactor);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR, newKc.statusStorageReplicationFactor);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_GROUP_ID, newKc.groupId);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_KEY_CONVERTER, newKc.keyConverter);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE, newKc.keyConverterSchemasEnable);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_VALUE_CONVERTER, newKc.valueConverter);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE, newKc.valueConverterSchemasEnable);
    }

    @Test
    public void testDiffNoDiffs() {
        ClusterDiffResult diff = kc.diff(kc.generateDeploymentConfig(), kc.generateSourceImageStream(), kc.generateTargetImageStream(), kc.generateBuildConfig());

        assertFalse(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffScaleUp() {
        DeploymentConfig dep = kc.generateDeploymentConfig();
        dep.getSpec().setReplicas(dep.getSpec().getReplicas() - 1);
        ClusterDiffResult diff = kc.diff(dep, kc.generateSourceImageStream(), kc.generateTargetImageStream(), kc.generateBuildConfig());

        assertFalse(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertTrue(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffScaleDown() {
        DeploymentConfig dep = kc.generateDeploymentConfig();
        dep.getSpec().setReplicas(dep.getSpec().getReplicas() + 1);
        ClusterDiffResult diff = kc.diff(dep, kc.generateSourceImageStream(), kc.generateTargetImageStream(), kc.generateBuildConfig());

        assertFalse(diff.isDifferent());
        assertTrue(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffSourceImageStream() {
        ImageStream sis = kc.generateSourceImageStream();
        kc.setImage("some/image:latest");
        ClusterDiffResult diff = kc.diff(kc.generateDeploymentConfig(), sis, kc.generateTargetImageStream(), kc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffBuildConfig() {
        BuildConfig bc = kc.generateBuildConfig();
        kc.setImage("my-image:tag");
        ClusterDiffResult diff = kc.diff(kc.generateDeploymentConfig(), kc.generateSourceImageStream(), kc.generateTargetImageStream(), bc);
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffLabels() {
        ClusterDiffResult diff;

        DeploymentConfig dep = kc.generateDeploymentConfig();
        dep.getMetadata().setLabels(Collections.EMPTY_MAP);
        diff = kc.diff(dep, kc.generateSourceImageStream(), kc.generateTargetImageStream(), kc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        ImageStream sis = kc.generateSourceImageStream();
        sis.getMetadata().setLabels(Collections.EMPTY_MAP);
        diff = kc.diff(kc.generateDeploymentConfig(), sis, kc.generateTargetImageStream(), kc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        ImageStream tis = kc.generateTargetImageStream();
        tis.getMetadata().setLabels(Collections.EMPTY_MAP);
        diff = kc.diff(kc.generateDeploymentConfig(), kc.generateSourceImageStream(), tis, kc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        BuildConfig bc = kc.generateBuildConfig();
        bc.getMetadata().setLabels(Collections.EMPTY_MAP);
        diff = kc.diff(kc.generateDeploymentConfig(), kc.generateSourceImageStream(), kc.generateTargetImageStream(), bc);
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffConfigOptions() {
        KafkaConnectS2ICluster newKc;
        ClusterDiffResult diff;

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                123, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, 123, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, "some-kafka-broker:9092", groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, "some-other-group-id", configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, 5, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, 5,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                5, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, "some-other-converter", valueConverter, keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, "some-other-converter", keyConverterSchemas, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, true, valuesConverterSchema));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());

        newKc = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createKafkaConnectS2IClusterConfigMap(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
                statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, true));
        diff = kc.diff(newKc.generateDeploymentConfig(), newKc.generateSourceImageStream(), newKc.generateTargetImageStream(), newKc.generateBuildConfig());
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
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
        assertEquals(1, svc.getSpec().getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testPatchService()   {
        Service orig = new ServiceBuilder()
                .withNewMetadata()
                    .withName(kc.kafkaConnectClusterName(cluster))
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                .endSpec()
                .build();

        Service svc = kc.patchService(orig);

        Map<String, String> expectedLabels = ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, svc.getMetadata().getLabels());
        assertEquals(expectedLabels, svc.getSpec().getSelector());
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
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
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
    public void testPatchDeploymentConfig()   {
        DeploymentConfig orig = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(namespace, cluster)).generateDeploymentConfig();
        orig.getMetadata().setLabels(Collections.EMPTY_MAP);
        orig.getSpec().getTemplate().getMetadata().setLabels(Collections.EMPTY_MAP);


        DeploymentConfig dep = kc.patchDeploymentConfig(orig);

        Map<String, String> expectedLabels = ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(new Integer(KafkaConnectS2ICluster.DEFAULT_REPLICAS), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
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
    public void testPatchBuildConfig() {
        BuildTriggerPolicy triggerConfigChange = new BuildTriggerPolicy();
        triggerConfigChange.setType("ConfigChange");

        BuildTriggerPolicy triggerImageChange = new BuildTriggerPolicy();
        triggerImageChange.setType("ImageChange");
        triggerImageChange.setImageChange(new ImageChangeTrigger());

        BuildConfig orig = new BuildConfigBuilder()
                .withNewMetadata()
                    .withName(kc.kafkaConnectClusterName(cluster))
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withFailedBuildsHistoryLimit(5)
                    .withNewOutput()
                        .withNewTo()
                            .withKind("ImageStreamTag")
                            .withName(kc.image)
                        .endTo()
                    .endOutput()
                    .withRunPolicy("Serial")
                    .withNewSource()
                        .withType("Binary")
                        .withBinary(new BinaryBuildSource())
                    .endSource()
                    .withNewStrategy()
                        .withType("Source")
                        .withNewSourceStrategy()
                            .withNewFrom()
                                .withKind("ImageStreamTag")
                                .withName("someimage:latest")
                            .endFrom()
                        .endSourceStrategy()
                    .endStrategy()
                    .withTriggers(triggerConfigChange, triggerImageChange)
                .endSpec()
                .build();

        BuildConfig bc = kc.patchBuildConfig(orig);

        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), bc.getMetadata().getLabels());
        assertEquals(kc.getSourceImageStreamName() + ":" + kc.sourceImageTag, bc.getSpec().getStrategy().getSourceStrategy().getFrom().getName());
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
    public void testPatchSourceImageStream() {
        ObjectReference origImage = new ObjectReference();
        origImage.setKind("DockerImage");
        origImage.setName("something:else");

        TagReference sourceTag = new TagReference();
        sourceTag.setName("something");
        sourceTag.setFrom(origImage);

        ImageStream orig = new ImageStreamBuilder()
                .withNewMetadata()
                    .withName(kc.getSourceImageStreamName())
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withLookupPolicy(new ImageLookupPolicyBuilder().withLocal(false).build())
                    .withTags(sourceTag)
                .endSpec()
                .build();

        ImageStream is = kc.patchSourceImageStream(orig);

        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.getSourceImageStreamName()), is.getMetadata().getLabels());
        assertEquals(image.substring(image.lastIndexOf(":") + 1), is.getSpec().getTags().get(0).getName());
        assertEquals(image, is.getSpec().getTags().get(0).getFrom().getName());
    }

    @Test
    public void testPatchTargetImageStream() {
        ImageStream orig = new ImageStreamBuilder()
                .withNewMetadata()
                    .withName(kc.kafkaConnectClusterName(cluster))
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withLookupPolicy(new ImageLookupPolicyBuilder().withLocal(false).build())
                .endSpec()
                .build();

        ImageStream is = kc.patchTargetImageStream(orig);

        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster)), is.getMetadata().getLabels());
    }
}

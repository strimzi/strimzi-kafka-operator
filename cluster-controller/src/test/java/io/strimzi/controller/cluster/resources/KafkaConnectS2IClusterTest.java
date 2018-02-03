/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.strimzi.controller.cluster.ResourceUtils;
import org.junit.Test;

import java.util.Collections;

import static io.strimzi.controller.cluster.ResourceUtils.labels;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaConnectS2IClusterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
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
    private final KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromConfigMap(true, cm);



    @Test
    public void testDefaultValues() {
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromConfigMap(true, ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(namespace, cluster));

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

    // TODO: Test from DeploymentConfig / ImageStream

    // TODO: Test Env Vars

    // TODO: Test diff

    @Test
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), svc.getMetadata().getLabels());
        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), svc.getSpec().getSelector());
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

        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), svc.getMetadata().getLabels());
        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), svc.getSpec().getSelector());
    }

    @Test
    public void testGenerateDeploymentConfig()   {
        DeploymentConfig dep = kc.generateDeploymentConfig();

        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), dep.getMetadata().getLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(kc.getEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
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
        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getSpec().getTriggers().get(1).getImageChangeParams().getContainerNames().get(0));
        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getName());
        assertEquals("ImageStreamTag", dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getKind());
    }

    @Test
    public void testPatchDeploymentConfig()   {
        DeploymentConfig orig = KafkaConnectS2ICluster.fromConfigMap(true, ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(namespace, cluster)).generateDeploymentConfig();
        orig.getMetadata().setLabels(Collections.EMPTY_MAP);
        orig.getSpec().getTemplate().getMetadata().setLabels(Collections.EMPTY_MAP);


        DeploymentConfig dep = kc.patchDeploymentConfig(orig);

        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), dep.getMetadata().getLabels());
        assertEquals(new Integer(KafkaConnectS2ICluster.DEFAULT_REPLICAS), dep.getSpec().getReplicas());
        assertEquals(ResourceUtils.labels("strimzi.io/cluster", cluster, "strimzi.io/type", "kafka-connect-s2i", "strimzi.io/kind", "cluster", "strimzi.io/name", kc.kafkaConnectClusterName(cluster)), dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(kc.getEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
    }

    // TODO: Test Build Config

    // TODO: Test ImageStreams

}

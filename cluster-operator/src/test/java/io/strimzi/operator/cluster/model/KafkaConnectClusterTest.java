/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssemblyBuilder;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.assembly.MockCertManager;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaConnectClusterTest {
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

    private CertManager certManager = new MockCertManager();
    private final KafkaConnectAssembly resource = new KafkaConnectAssemblyBuilder(ResourceUtils.createEmptyKafkaConnectCluster(namespace, cluster))
            .withNewSpec()
            .withMetrics((Map<String, Object>) TestUtils.fromJson(metricsCmJson, Map.class))
            .withConfig((Map<String, Object>) TestUtils.fromJson(configurationJson, Map.class))
            .withImage(image)
            .withReplicas(replicas)
            .withReadinessProbe(new Probe(healthDelay, healthTimeout))
            .withLivenessProbe(new Probe(healthDelay, healthTimeout))
            .endSpec()
            .build();
    private final KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(resource);

    @Rule
    public ResourceTester<KafkaConnectAssembly, KafkaConnectCluster> resourceTester = new ResourceTester<>(KafkaConnectAssembly.class, KafkaConnectCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster, Labels.STRIMZI_TYPE_LABEL, "kafka-connect", "my-user-label", "cromulent", Labels.STRIMZI_NAME_LABEL, name, Labels.STRIMZI_KIND_LABEL, KafkaConnectAssembly.RESOURCE_KIND);
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(kc.kafkaConnectClusterName(cluster));
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
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(ResourceUtils.createEmptyKafkaConnectCluster(namespace, cluster));

        assertEquals(KafkaConnectCluster.DEFAULT_IMAGE, kc.image);
        assertEquals(KafkaConnectCluster.DEFAULT_REPLICAS, kc.replicas);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY, kc.readinessInitialDelay);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.readinessTimeout);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY, kc.livenessInitialDelay);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.livenessTimeout);
        assertEquals(defaultConfiguration, kc.getConfiguration().getConfiguration());
    }

    @Test
    public void testFromConfigMap() {
        assertEquals(replicas, kc.replicas);
        assertEquals(image, kc.image);
        assertEquals(healthDelay, kc.readinessInitialDelay);
        assertEquals(healthTimeout, kc.readinessTimeout);
        assertEquals(healthDelay, kc.livenessInitialDelay);
        assertEquals(healthTimeout, kc.livenessTimeout);
        assertEquals(expectedConfiguration, kc.getConfiguration().getConfiguration());
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), kc.getEnvVars());
    }

    @Test
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        assertEquals(expectedLabels(kc.getServiceName()), svc.getMetadata().getLabels());
        assertEquals(expectedLabels(), svc.getSpec().getSelector());
        assertEquals(2, svc.getSpec().getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
        assertEquals(kc.getPrometheusAnnotations(), svc.getMetadata().getAnnotations());
    }

    @Test
    public void testGenerateDeployment()   {
        Deployment dep = kc.generateDeployment();

        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = expectedLabels();
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(kc.kafkaConnectClusterName(this.cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(kc.image, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());
        assertEquals("RollingUpdate", dep.getSpec().getStrategy().getType());
        assertEquals(new Integer(1), dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal());
        assertEquals(new Integer(0), dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal());
    }

    @Test
    public void withAffinity() throws IOException {
        resourceTester
            .assertDesiredResource("-Deployment.yaml", kcc -> kcc.generateDeployment().getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withTolerations() throws IOException {
        resourceTester
            .assertDesiredResource("-Deployment.yaml", kcc -> kcc.generateDeployment().getSpec().getTemplate().getSpec().getTolerations());
    }
}

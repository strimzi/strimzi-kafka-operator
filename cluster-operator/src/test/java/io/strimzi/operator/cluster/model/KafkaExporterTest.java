/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaExporterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaExporterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> kafkaConfig = singletonMap("foo", "bar");
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");
    private final Storage kafkaStorage = new EphemeralStorage();
    private final SingleVolumeStorage zkStorage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private final String exporterOperatorLogging = "debug";
    private final String version = "2.3.0";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final String keImage = "my-exporter-image";
    private final String groupRegex = "my-group-.*";
    private final String topicRegex = "my-topic-.*";

    private final KafkaExporterSpec exporterOperator = new KafkaExporterSpecBuilder()
            .withLogging(exporterOperatorLogging)
            .withGroupRegex(groupRegex)
            .withTopicRegex(topicRegex)
            .withImage(keImage)
            .withEnableSaramaLogging(true)
            .build();
    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .editKafka()
                    .withVersion(version)
                    .endKafka()
                    .withKafkaExporter(exporterOperator)
                    .endSpec()
                    .build();
    private final KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertEquals(1, resource.getMetadata().getOwnerReferences().size());
        assertEquals(ownerRef, resource.getMetadata().getOwnerReferences().get(0));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.KUBERNETES_NAME_LABEL, Labels.KUBERNETES_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaExporterResources.deploymentName(cluster));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_LOGGING).withValue(exporterOperatorLogging).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION).withValue(version).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX).withValue(groupRegex).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX).withValue(topicRegex).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER).withValue("foo-kafka-bootstrap:" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA).withValue("true").build());
        return expected;
    }

    @Test
    public void testFromConfigMapDefaultConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, null,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, new KafkaExporterSpec());
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);
        assertEquals(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE, ke.getImage());
        assertEquals("info", ke.logging);
        assertEquals(".*", ke.groupRegex);
        assertEquals(".*", ke.topicRegex);
        assertEquals(false, ke.saramaLoggingEnabled);
    }

    @Test
    public void testFromConfigMap() {
        Assert.assertEquals(namespace, ke.namespace);
        Assert.assertEquals(cluster, ke.cluster);
        assertEquals(keImage, ke.getImage());
        assertEquals("debug", ke.logging);
        assertEquals("my-group-.*", ke.groupRegex);
        assertEquals("my-topic-.*", ke.topicRegex);
        assertEquals(true, ke.saramaLoggingEnabled);

    }

    @Test
    public void testGenerateDeployment() {
        Deployment dep = ke.generateDeployment(true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(1, containers.size());

        Assert.assertEquals(KafkaExporterResources.deploymentName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(1, dep.getMetadata().getOwnerReferences().size());
        assertEquals(ke.createOwnerReference(), dep.getMetadata().getOwnerReferences().get(0));

        // checks on the main Exporter container
        assertEquals(ke.image, containers.get(0).getImage());
        assertEquals(getExpectedEnvVars(), containers.get(0).getEnv());
        assertEquals(1, containers.get(0).getPorts().size());
        assertEquals(KafkaExporter.METRICS_PORT_NAME, containers.get(0).getPorts().get(0).getName());
        assertEquals("TCP", containers.get(0).getPorts().get(0).getProtocol());
        assertEquals("RollingUpdate", dep.getSpec().getStrategy().getType());

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertEquals(2, volumes.size());

        Volume volume = volumes.stream().filter(vol -> KafkaExporter.CLUSTER_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertNotNull(volume);
        assertEquals(KafkaResources.clusterCaCertificateSecretName(cluster), volume.getSecret().getSecretName());

        volume = volumes.stream().filter(vol -> KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertNotNull(volume);
        assertEquals(KafkaExporterResources.secretName(cluster), volume.getSecret().getSecretName());

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertEquals(2, volumesMounts.size());

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> KafkaExporter.CLUSTER_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertNotNull(volumeMount);
        assertEquals(KafkaExporter.CLUSTER_CA_CERTS_VOLUME_MOUNT, volumeMount.getMountPath());

        volumeMount = volumesMounts.stream().filter(vol -> KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertNotNull(volumeMount);
        assertEquals(KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_MOUNT, volumeMount.getMountPath());

    }

    @Test
    public void testEnvVars()   {
        Assert.assertEquals(getExpectedEnvVars(), ke.getEnvVars());
    }

    @Rule
    public ResourceTester<Kafka, KafkaExporter> helper = new ResourceTester<>(Kafka.class, VERSIONS, KafkaExporter::fromCrd);

    @Test
    public void testImagePullPolicy() {
        Deployment dep = ke.generateDeployment(true, ImagePullPolicy.ALWAYS, null);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());

        dep = ke.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
    }

    @Test
    public void testContainerTemplateEnvVars() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        KafkaExporterSpec exporterSpec = new KafkaExporterSpecBuilder()
                .withLogging(exporterOperatorLogging)
                .withGroupRegex(groupRegex)
                .withTopicRegex(topicRegex)
                .withImage(keImage)
                .withNewTemplate()
                    .withNewContainer()
                        .withEnv(envVar1, envVar2)
                    .endContainer()
                .endTemplate()
                .build();

        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, exporterSpec);
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);

        List<EnvVar> kafkaEnvVars = ke.getEnvVars();
        assertEquals(testEnvOneValue, kafkaEnvVars.stream().filter(var -> testEnvOneKey.equals(var.getName())).map(EnvVar::getValue).findFirst().get());
        assertEquals(testEnvTwoValue, kafkaEnvVars.stream().filter(var -> testEnvTwoKey.equals(var.getName())).map(EnvVar::getValue).findFirst().get());
    }

    @Test
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX;
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        KafkaExporterSpec exporterSpec = new KafkaExporterSpecBuilder()
                .withLogging(exporterOperatorLogging)
                .withGroupRegex(groupRegex)
                .withTopicRegex(topicRegex)
                .withImage(keImage)
                .withNewTemplate()
                    .withNewContainer()
                        .withEnv(envVar1, envVar2)
                    .endContainer()
                .endTemplate()
                .build();

        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, exporterSpec);
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);

        List<EnvVar> kafkaEnvVars = ke.getEnvVars();
        assertEquals(testEnvOneValue, kafkaEnvVars.stream().filter(var -> testEnvOneKey.equals(var.getName())).map(EnvVar::getValue).findFirst().get());
        assertEquals(groupRegex, kafkaEnvVars.stream().filter(var -> testEnvTwoKey.equals(var.getName())).map(EnvVar::getValue).findFirst().get());
    }

    @Test
    public void testExporterNotDeployed() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, null);
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);

        assertNull(ke.generateDeployment(true, null, null));
        assertNull(ke.generateService());
        assertNull(ke.generateSecret(null));
    }

    @Test
    public void testGenerateService()   {
        Service svc = ke.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        assertEquals(expectedLabels(ke.getServiceName()), svc.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), svc.getSpec().getSelector());
        assertEquals(1, svc.getSpec().getPorts().size());
        assertEquals(AbstractModel.METRICS_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.METRICS_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
        assertEquals(ke.getPrometheusAnnotations(), svc.getMetadata().getAnnotations());

        checkOwnerReference(ke.createOwnerReference(), svc);
    }

    @Test
    public void testGenerateServiceWhenDisabled()   {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, null);
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);

        assertNull(ke.generateService());
    }

    @Test
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewKafkaExporter()
                        .withNewTemplate()
                            .withNewDeployment()
                                .withNewMetadata()
                                    .withLabels(depLabels)
                                    .withAnnotations(depAnots)
                                .endMetadata()
                            .endDeployment()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnots)
                                .endMetadata()
                                .withNewPriorityClassName("top-priority")
                                .withNewSchedulerName("my-scheduler")
                            .endPod()
                            .withNewService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnots)
                                .endMetadata()
                            .endService()
                        .endTemplate()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);

        // Check Deployment
        Deployment dep = ke.generateDeployment(true, null, null);
        assertTrue(dep.getMetadata().getLabels().entrySet().containsAll(depLabels.entrySet()));
        assertTrue(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()));

        // Check Pods
        assertTrue(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()));
        assertTrue(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()));
        assertEquals("top-priority", dep.getSpec().getTemplate().getSpec().getPriorityClassName());
        assertEquals("my-scheduler", dep.getSpec().getTemplate().getSpec().getSchedulerName());

        // Check Service
        Service svc = ke.generateService();
        assertTrue(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()));
        assertTrue(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()));
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}

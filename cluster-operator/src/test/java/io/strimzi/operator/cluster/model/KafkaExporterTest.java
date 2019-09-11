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
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.TestUtils.map;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
    private final String version = "2.1.0";
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(
            new StringReader(
                    "2.0.0  default  2.0  2.0  1234567890abcdef 2.0.x\n" +
                            "2.0.1           2.0  2.0  1234567890abcdef 2.0.x\n" +
                            "2.1.0           2.1  2.1  1234567890abcdef 2.1.x\n"),
            map("2.0.0", "strimzi/kafka:0.8.0-kafka-2.0.0",
                    "2.0.1", "strimzi/kafka:0.8.0-kafka-2.0.1",
                    "2.1.0", "strimzi/kafka:0.8.0-kafka-2.1.0"),
            singletonMap("2.0.0", "kafka-connect"),
            singletonMap("2.0.0", "kafka-connect-s2i"),
            singletonMap("2.0.0", "kafka-mirror-maker-s2i")) { };

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final String keWatchedNamespace = "my-exporter-namespace";
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
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, new KafkaExporterSpec());
        KafkaExporter ke = KafkaExporter.fromCrd(resource, VERSIONS);
        assertEquals("strimzi/kafka:0.8.0-kafka-2.0.0", ke.getImage());
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

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}

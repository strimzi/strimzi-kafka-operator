/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaExporterSpecBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
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
    private final String version = "2.3.0";

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final String keWatchedNamespace = "my-exporter-namespace";
    private final String keImage = "my-exporter-image";
    private final String groupRegex = ".+";
    private final String topicRegex = ".*";


    private final KafkaExporterSpec exporterOperator = new KafkaExporterSpecBuilder()
            .withLogging(exporterOperatorLogging)
            .withGroupRegex(groupRegex)
            .withTopicRegex(topicRegex)
            .withImage(keImage)
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
    private final KafkaExporter ke = KafkaExporter.fromCrd(resource);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_LOGGING).withValue(exporterOperatorLogging).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION).withValue(version).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX).withValue(groupRegex).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX).withValue(topicRegex).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER).withValue("foo-kafka-bootstrap:9092").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA).withValue("false").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_STRIMZI_LIVENESS_PERIOD).withValue("10").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_STRIMZI_READINESS_PERIOD).withValue("10").build());
        return expected;
    }

    @Test
    public void testFromConfigMapNoConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, null, kafkaLogJson, zooLogJson);
        KafkaExporter ke = KafkaExporter.fromCrd(resource);
        assertNull(ke);
    }

    @Test
    public void testFromConfigMapDefaultConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, null, kafkaLogJson, zooLogJson, new KafkaExporterSpec());
        KafkaExporter ke = KafkaExporter.fromCrd(resource);
        Assert.assertEquals(null, ke.getImage());
        assertNull(ke.getLogging());
    }

    @Test
    public void testFromConfigMap() {

        Assert.assertEquals(namespace, ke.namespace);
        Assert.assertEquals(cluster, ke.cluster);
        assertEquals(keImage, ke.image);
        assertEquals(keImage, ke.getImage());
    }

    @Test
    public void testGenerateDeployment() {

        Deployment dep = ke.generateDeployment(true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(1, containers.size());

        Assert.assertEquals(ke.getExporterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(1, dep.getMetadata().getOwnerReferences().size());
        assertEquals(ke.createOwnerReference(), dep.getMetadata().getOwnerReferences().get(0));

        // checks on the main Exporter container
        assertEquals(ke.image, containers.get(0).getImage());
        assertEquals(getExpectedEnvVars(), containers.get(0).getEnv());
        assertEquals(1, containers.get(0).getPorts().size());
        assertEquals(KafkaExporter.METRICS_PORT_NAME, containers.get(0).getPorts().get(0).getName());
        assertEquals("TCP", containers.get(0).getPorts().get(0).getProtocol());
        assertEquals("Recreate", dep.getSpec().getStrategy().getType());
    }

    @Test
    public void testEnvVars()   {
        Assert.assertEquals(getExpectedEnvVars(), ke.getEnvVars());
    }

    @Rule
    public ResourceTester<Kafka, KafkaExporter> helper = new ResourceTester<>(Kafka.class, KafkaExporter::fromCrd);

    @Test
    public void testImagePullPolicy() {
        Deployment dep = ke.generateDeployment(true, ImagePullPolicy.ALWAYS, null);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());

        dep = ke.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}

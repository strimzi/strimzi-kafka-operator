/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.BridgeMetricsConversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.MultipartConversions;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
public class ConvertResourceCommandIT {
    private static final String NAMESPACE = "api-conversion";

    private static KubernetesClient client;

    @BeforeEach
    public void beforeEach()    {
        MultipartConversions.remove();
    }

    @BeforeAll
    static void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        ConversionTestUtils.createV1beta2Crds(client);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    static void teardownEnvironment() {
        ConversionTestUtils.deleteAllCrds(client);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }

    @Test
    public void testNamedConversion() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            Kafka kafka2 = new KafkaBuilder(kafka1).editMetadata().withName("kafka2").endMetadata().build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka2)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--name", "kafka1", "--namespace", NAMESPACE);
            String commandOutput = sw.toString();
            System.out.println(commandOutput);
            assertThat(exitCode, is(0));

            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
        }
    }

    @Test
    public void testMultiResourceConversion() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.bridgeOperation(client);

        try {
            KafkaBridge bridge = new KafkaBridgeBuilder()
                    .withNewMetadata()
                        .withName("bridge1")
                    .endMetadata()
                    .withNewSpec()
                        .withBootstrapServers("localhost:9092")
                        .withNewHttp()
                            .withPort(8080)
                        .endHttp()
                        .withEnableMetrics(true)
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(bridge)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "KafkaBridge", "--name", "bridge1", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));
            assertThat(sw.toString(), containsString("Converting KafkaBridge resource named bridge1 from namespace " + NAMESPACE));
            assertThat(sw.toString(), containsString("KafkaBridge resource named bridge1 in namespace " + NAMESPACE + " has been converted"));

            KafkaBridge actualBridge = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("bridge1").get(), KafkaBridge.class);

            assertThat(actualBridge.getSpec().getEnableMetrics(), is(nullValue()));
            assertThat(actualBridge.getSpec().getMetricsConfig(), is(notNullValue()));
            assertThat(actualBridge.getSpec().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
            JmxPrometheusExporterMetrics metricsConfig = (JmxPrometheusExporterMetrics) actualBridge.getSpec().getMetricsConfig();
            assertThat(metricsConfig.getValueFrom().getConfigMapKeyRef().getName(), is("bridge1-bridge-jmx-exporter-configuration"));
            assertThat(metricsConfig.getValueFrom().getConfigMapKeyRef().getKey(), is("bridge1-bridge-jmx-exporter-configuration.yaml"));

            ConfigMap kafkaMetrics = client.configMaps().inNamespace(NAMESPACE).withName("bridge1-bridge-jmx-exporter-configuration").get();
            assertThat(kafkaMetrics, is(notNullValue()));
            assertThat(kafkaMetrics.getData().get("bridge1-bridge-jmx-exporter-configuration.yaml"), is(BridgeMetricsConversion.defaultMetricsConfiguration()));
        } finally {
            op.inNamespace(NAMESPACE).withName("bridge1").delete();
            client.configMaps().inNamespace(NAMESPACE).withName("bridge1-bridge-jmx-exporter-configuration").delete();
        }
    }

    @Test
    public void testMultipleResourcesConversion() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("4.1.0")
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            Kafka kafka2 = new KafkaBuilder(kafka1)
                    .editMetadata()
                        .withName("kafka2")
                    .endMetadata()
                    .build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka2)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));

            Kafka actualKafka2 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka2").get(), Kafka.class);
            assertThat(actualKafka2.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getStorage(), is(nullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
        }
    }

    @Test
    public void testMultipleKindsConversion() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> kafkaOp = ConversionTestUtils.kafkaOperation(client);
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> bridgeOp = ConversionTestUtils.bridgeOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("4.1.0")
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            KafkaBridge bridge = new KafkaBridgeBuilder()
                    .withNewMetadata()
                        .withName("bridge1")
                    .endMetadata()
                    .withNewSpec()
                        .withBootstrapServers("localhost:9092")
                        .withNewHttp()
                            .withPort(8080)
                        .endHttp()
                        .withEnableMetrics(true)
                    .endSpec()
                    .build();

            kafkaOp.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            bridgeOp.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(bridge)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--kind", "KafkaBridge", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, containsString("Converting KafkaBridge resource named bridge1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("KafkaBridge resource named bridge1 in namespace " + NAMESPACE + " has been converted"));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(kafkaOp.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));

            KafkaBridge actualBridge = ConversionTestUtils.genericToTyped(bridgeOp.inNamespace(NAMESPACE).withName("bridge1").get(), KafkaBridge.class);

            assertThat(actualBridge.getSpec().getEnableMetrics(), is(nullValue()));
            assertThat(actualBridge.getSpec().getMetricsConfig(), is(notNullValue()));
            assertThat(actualBridge.getSpec().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
            JmxPrometheusExporterMetrics metricsConfig = (JmxPrometheusExporterMetrics) actualBridge.getSpec().getMetricsConfig();
            assertThat(metricsConfig.getValueFrom().getConfigMapKeyRef().getName(), is("bridge1-bridge-jmx-exporter-configuration"));
            assertThat(metricsConfig.getValueFrom().getConfigMapKeyRef().getKey(), is("bridge1-bridge-jmx-exporter-configuration.yaml"));
        } finally {
            kafkaOp.inNamespace(NAMESPACE).withName("kafka1").delete();
            bridgeOp.inNamespace(NAMESPACE).withName("bridge1").delete();
        }
    }

    @Test
    public void testFailingKafkaConversion() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("4.1.0")
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            Kafka kafka2 = new KafkaBuilder(kafka1)
                    .editMetadata()
                        .withName("kafka2")
                    .endMetadata()
                    .editSpec()
                        .editKafka()
                            .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("16Gi"), "cpu", new Quantity("8"))).build())
                        .endKafka()
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka2)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--namespace", NAMESPACE);
            assertThat(exitCode, is(1));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Failed to convert Kafka resource named kafka2 in namespace " + NAMESPACE + ": The resources configuration in .spec.kafka.resources is removed in the v1 API version. Use the KafkaNodePool resource instead to configure resources. Please fix the resource manually and re-run the conversion tool."));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));

            Kafka actualKafka2 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka2").get(), Kafka.class);
            assertThat(actualKafka2.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka2.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
        }
    }

    @Test
    public void testAllKindsConversion() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> kafkaOp = ConversionTestUtils.kafkaOperation(client);
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> bridgeOp = ConversionTestUtils.bridgeOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("4.1.0")
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            KafkaBridge bridge = new KafkaBridgeBuilder()
                    .withNewMetadata()
                        .withName("bridge1")
                    .endMetadata()
                    .withNewSpec()
                        .withBootstrapServers("localhost:9092")
                        .withNewHttp()
                            .withPort(8080)
                        .endHttp()
                        .withEnableMetrics(true)
                    .endSpec()
                    .build();

            kafkaOp.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            bridgeOp.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(bridge)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, containsString("Converting KafkaBridge resource named bridge1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("KafkaBridge resource named bridge1 in namespace " + NAMESPACE + " has been converted"));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(kafkaOp.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));

            KafkaBridge actualBridge = ConversionTestUtils.genericToTyped(bridgeOp.inNamespace(NAMESPACE).withName("bridge1").get(), KafkaBridge.class);

            assertThat(actualBridge.getSpec().getEnableMetrics(), is(nullValue()));
            assertThat(actualBridge.getSpec().getMetricsConfig(), is(notNullValue()));
            assertThat(actualBridge.getSpec().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
            JmxPrometheusExporterMetrics metricsConfig = (JmxPrometheusExporterMetrics) actualBridge.getSpec().getMetricsConfig();
            assertThat(metricsConfig.getValueFrom().getConfigMapKeyRef().getName(), is("bridge1-bridge-jmx-exporter-configuration"));
            assertThat(metricsConfig.getValueFrom().getConfigMapKeyRef().getKey(), is("bridge1-bridge-jmx-exporter-configuration.yaml"));
        } finally {
            kafkaOp.inNamespace(NAMESPACE).withName("kafka1").delete();
            bridgeOp.inNamespace(NAMESPACE).withName("bridge1").delete();
        }
    }

    @Test
    public void tesAllNamespacesConversion() {
        String namespace2 = NAMESPACE + "2";
        TestUtils.createNamespace(client, namespace2);

        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("4.1.0")
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            Kafka kafka2 = new KafkaBuilder(kafka1)
                    .editMetadata()
                        .withName("kafka2")
                    .endMetadata()
                    .build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            op.inNamespace(namespace2).resource(ConversionTestUtils.typedToGeneric(kafka2)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--all-namespaces");
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka2 from namespace " + namespace2));
            assertThat(commandOutput, containsString("Kafka resource named kafka2 in namespace " + namespace2 + " has been converted"));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));

            Kafka actualKafka2 = ConversionTestUtils.genericToTyped(op.inNamespace(namespace2).withName("kafka2").get(), Kafka.class);
            assertThat(actualKafka2.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getStorage(), is(nullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(namespace2).withName("kafka2").delete();
            TestUtils.deleteNamespace(client, namespace2);
        }
    }

    @Test
    public void testWrongNamespaceOrKindConversion() {
        String namespace2 = NAMESPACE + "-other";
        TestUtils.createNamespace(client, namespace2);

        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("4.1.0")
                            .withReplicas(3)
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            Kafka kafka2 = new KafkaBuilder(kafka1)
                    .editMetadata()
                        .withName("kafka2")
                    .endMetadata()
                    .build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();
            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka2)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--namespace", namespace2);
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted")));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka2 in namespace " + NAMESPACE + " has been converted")));

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));

            Kafka actualKafka2 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka2").get(), Kafka.class);
            assertThat(actualKafka2.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka2.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));

            exitCode = cmd.execute("convert-resource", "--kind", "Kafka");
            assertThat(exitCode, is(0));

            commandOutput = sw.toString();
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted")));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka2 in namespace " + NAMESPACE + " has been converted")));

            actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));

            actualKafka2 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka2").get(), Kafka.class);
            assertThat(actualKafka2.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka2.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));

            exitCode = cmd.execute("convert-resource", "--kind", "KafkaUser", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            commandOutput = sw.toString();
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted")));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka2 in namespace " + NAMESPACE + " has been converted")));

            actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));

            actualKafka2 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka2").get(), Kafka.class);
            assertThat(actualKafka2.getSpec().getZookeeper(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getReplicas(), is(3));
            assertThat(actualKafka2.getSpec().getKafka().getStorage(), is(new EphemeralStorage()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
            TestUtils.deleteNamespace(client, namespace2);
        }
    }

    @Test
    public void testNamespaceAndAllNamespaces() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cmd.setOut(pw);
        cmd.setErr(pw);

        int exitCode = cmd.execute("convert-resource", "--all-namespaces", "--namespace", NAMESPACE);
        assertThat(exitCode, is(2));
        assertThat(sw.toString(), containsString("--namespace=<namespace>, --all-namespaces are mutually exclusive"));
    }

    @Test
    public void testNamespaceWithoutNamespace() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cmd.setOut(pw);
        cmd.setErr(pw);

        int exitCode = cmd.execute("convert-resource", "--namespace");
        assertThat(exitCode, is(2));
        assertThat(sw.toString(), containsString("Missing required parameter for option '--namespace'"));
    }

    @Test
    public void testNameWithoutKind() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cmd.setOut(pw);
        cmd.setErr(pw);

        int exitCode = cmd.execute("convert-resource", "--name", "my-cluster");
        assertThat(exitCode, is(1));
        assertThat(sw.toString(), containsString("The --name option can be used only with --namespace option and single --kind option"));
    }

    @Test
    public void testNameWithTooManyKinds() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cmd.setOut(pw);
        cmd.setErr(pw);

        int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--kind", "KafkaTopic", "--name", "my-cluster");
        assertThat(exitCode, is(1));
        assertThat(sw.toString(), containsString("The --name option can be used only with --namespace option and single --kind option"));
    }

    @Test
    public void testWithWrongKind() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cmd.setOut(pw);
        cmd.setErr(pw);

        int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--kind", "Monkey");
        assertThat(exitCode, is(1));
        assertThat(sw.toString(), containsString("Only valid Strimzi custom resource Kinds can be used"));
    }
}

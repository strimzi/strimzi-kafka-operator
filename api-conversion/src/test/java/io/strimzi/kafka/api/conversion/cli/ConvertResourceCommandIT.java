/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.kafka.api.conversion.converter.MultipartConversions;
import io.strimzi.kafka.api.conversion.utils.LegacyCrds;
import io.strimzi.test.k8s.KubeClusterResource;
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

public class ConvertResourceCommandIT {
    private static final String NAMESPACE = "api-conversion";
    private static final KubeClusterResource CLUSTER = KubeClusterResource.getInstance();
    private final KubernetesClient client = new DefaultKubernetesClient();

    @BeforeEach
    public void beforeEach()    {
        MultipartConversions.remove();
        CLUSTER.cluster();
    }

    @BeforeAll
    static void setupEnvironment() {
        CLUSTER.createNamespace(NAMESPACE);
        CliTestUtils.setupAllCrds(CLUSTER);
    }

    @AfterAll
    static void teardownEnvironment() {
        CliTestUtils.deleteAllCrds(CLUSTER);
        CLUSTER.deleteNamespaces();
    }

    @Test
    public void testNamedConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
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

            op.inNamespace(NAMESPACE).create(kafka1);
            op.inNamespace(NAMESPACE).create(kafka2);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--name", "kafka1", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

            Kafka actualKafka2 = op.inNamespace(NAMESPACE).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
        }
    }

    @Test
    public void testMultiResourceConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withMetrics(Map.of("x1", "y1", "x2", "y2"))
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                            .withMetrics(Map.of("x3", "y3", "x4", "y4"))
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).create(kafka1);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--name", "kafka1", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));
            assertThat(sw.toString(), containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(sw.toString(), containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();

            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

            assertThat(actualKafka1.getSpec().getKafka().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig().getType(), is("jmxPrometheusExporter"));
            assertThat(((JmxPrometheusExporterMetrics) actualKafka1.getSpec().getKafka().getMetricsConfig()).getValueFrom().getConfigMapKeyRef().getKey(), is("kafka1-kafka-jmx-exporter-configuration.yaml"));
            assertThat(((JmxPrometheusExporterMetrics) actualKafka1.getSpec().getKafka().getMetricsConfig()).getValueFrom().getConfigMapKeyRef().getName(), is("kafka1-kafka-jmx-exporter-configuration"));

            assertThat(actualKafka1.getSpec().getZookeeper().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig().getType(), is("jmxPrometheusExporter"));
            assertThat(((JmxPrometheusExporterMetrics) actualKafka1.getSpec().getZookeeper().getMetricsConfig()).getValueFrom().getConfigMapKeyRef().getKey(), is("kafka1-zookeeper-jmx-exporter-configuration.yaml"));
            assertThat(((JmxPrometheusExporterMetrics) actualKafka1.getSpec().getZookeeper().getMetricsConfig()).getValueFrom().getConfigMapKeyRef().getName(), is("kafka1-zookeeper-jmx-exporter-configuration"));

            ConfigMap kafkaMetrics = client.configMaps().inNamespace(NAMESPACE).withName("kafka1-kafka-jmx-exporter-configuration").get();
            assertThat(kafkaMetrics, is(notNullValue()));
            assertThat(kafkaMetrics.getData().get("kafka1-kafka-jmx-exporter-configuration.yaml"), is("x3: \"y3\"\nx4: \"y4\"\n"));

            ConfigMap zooMetrics = client.configMaps().inNamespace(NAMESPACE).withName("kafka1-zookeeper-jmx-exporter-configuration").get();
            assertThat(zooMetrics, is(notNullValue()));
            assertThat(zooMetrics.getData().get("kafka1-zookeeper-jmx-exporter-configuration.yaml"), is("x1: \"y1\"\nx2: \"y2\"\n"));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
        }
    }

    @Test
    public void testMultipleResourcesConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
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

            op.inNamespace(NAMESPACE).create(kafka1);
            op.inNamespace(NAMESPACE).create(kafka2);

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

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

            Kafka actualKafka2 = op.inNamespace(NAMESPACE).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
        }
    }

    @Test
    public void testMultipleKindsConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOp = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());
        MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> connectOp = LegacyCrds.kafkaConnectS2iOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
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

            Affinity affinity = new AffinityBuilder()
                    .withNewNodeAffinity()
                        .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                    .addNewMatchExpression()
                                        .withKey("dedicated")
                                        .withOperator("In")
                                        .withValues("Kafka")
                                    .endMatchExpression()
                                    .build())
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity()
                    .build();

            KafkaConnectS2I connect1 = new KafkaConnectS2IBuilder()
                    .withNewMetadata()
                        .withName("connect1")
                    .endMetadata()
                    .withNewSpec()
                        .withVersion("2.7.0")
                        .withReplicas(1)
                        .withBootstrapServers("my-kafka:9092")
                        .withAffinity(affinity)
                    .endSpec()
                    .build();

            kafkaOp.inNamespace(NAMESPACE).create(kafka1);
            connectOp.inNamespace(NAMESPACE).create(connect1);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--kind", "KafkaConnectS2I", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted"));
            assertThat(commandOutput, containsString("Converting KafkaConnectS2I resource named connect1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("KafkaConnectS2I resource named connect1 in namespace " + NAMESPACE + " has been converted"));

            Kafka actualKafka1 = kafkaOp.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

            KafkaConnectS2I actualConnect1 = connectOp.inNamespace(NAMESPACE).withName("connect1").get();
            assertThat(actualConnect1.getSpec().getAffinity(), is(nullValue()));
            assertThat(actualConnect1.getSpec().getTemplate().getPod().getAffinity(), is(affinity));

        } finally {
            kafkaOp.inNamespace(NAMESPACE).withName("kafka1").delete();
            connectOp.inNamespace(NAMESPACE).withName("connect1").delete();
        }
    }

    @Test
    public void testFailingTopicOperatorConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                        .endEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endSpec()
                    .build();

            Kafka kafka2 = new KafkaBuilder(kafka1)
                    .editMetadata()
                        .withName("kafka2")
                    .endMetadata()
                    .editSpec()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                            .endEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).create(kafka1);
            op.inNamespace(NAMESPACE).create(kafka2);

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
            assertThat(commandOutput, containsString("Failed to convert Kafka resource named kafka2 in namespace " + NAMESPACE + ": Cannot move /spec/topicOperator to /spec/entityOperator/topicOperator. The target path already exists. Please resolve the issue manually and run the API conversion tool again."));

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));
            assertThat(actualKafka1.getSpec().getEntityOperator().getTopicOperator(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getTopicOperator(), is(nullValue()));

            Kafka actualKafka2 = op.inNamespace(NAMESPACE).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getTopicOperator(), is(notNullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
        }
    }

    @Test
    public void testFailingAffinityConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

        try {
            Affinity affinity = new AffinityBuilder()
                    .withNewNodeAffinity()
                        .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                    .addNewMatchExpression()
                                        .withKey("dedicated")
                                        .withOperator("In")
                                        .withValues("FailingKafkaAffinity")
                                    .endMatchExpression()
                                    .build())
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity()
                    .build();

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                            .withAffinity(affinity)
                            .withNewTemplate()
                                .withNewPod()
                                    .withAffinity(affinity)
                                .endPod()
                            .endTemplate()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).create(kafka1);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--namespace", NAMESPACE);
            assertThat(exitCode, is(1));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Failed to convert Kafka resource named kafka1 in namespace " + NAMESPACE + ": Cannot move /spec/kafka/affinity to /spec/kafka/template/pod/affinity. The target path already exists. Please resolve the issue manually and run the API conversion tool again."));

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getTemplate().getPod().getAffinity(), is(affinity));
            assertThat(actualKafka1.getSpec().getKafka().getAffinity(), is(affinity));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
        }
    }

    @Test
    public void testFailingLoadBalancerSourceRangesConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                            .withNewTemplate()
                                .withNewExternalBootstrapService()
                                    .withLoadBalancerSourceRanges("88.208.76.87/32")
                                .endExternalBootstrapService()
                                .withNewPerPodService()
                                    .withLoadBalancerSourceRanges("165.39.29.204/32")
                                .endPerPodService()
                            .endTemplate()
                        .endKafka()
                        .withNewEntityOperator()
                            .withNewUserOperator()
                            .endUserOperator()
                            .withNewTopicOperator()
                            .endTopicOperator()
                        .endEntityOperator()
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).create(kafka1);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--namespace", NAMESPACE);
            assertThat(exitCode, is(1));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("Failed to convert Kafka resource named kafka1 in namespace " + NAMESPACE + ": KafkaClusterSpec's ExternalBootstrapService and PerPodService (fields externalTrafficPolicy and/or loadBalancerSourceRanges) are not equal and cannot be converted automatically! Please resolve the issue manually and run the API conversion tool again."));

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getTemplate().getExternalBootstrapService().getLoadBalancerSourceRanges().get(0), is("88.208.76.87/32"));
            assertThat(actualKafka1.getSpec().getKafka().getTemplate().getPerPodService().getLoadBalancerSourceRanges().get(0), is("165.39.29.204/32"));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
        }
    }

    @Test
    public void testAllKindsConversion() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOp = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());
        MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> connectOp = LegacyCrds.kafkaConnectS2iOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
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

            Affinity affinity = new AffinityBuilder()
                    .withNewNodeAffinity()
                        .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                    .addNewMatchExpression()
                                        .withKey("dedicated")
                                        .withOperator("In")
                                        .withValues("KafkaAllKinds")
                                    .endMatchExpression()
                                    .build())
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity()
                    .build();

            KafkaConnectS2I connect1 = new KafkaConnectS2IBuilder()
                    .withNewMetadata()
                        .withName("connect1")
                    .endMetadata()
                    .withNewSpec()
                        .withVersion("2.7.0")
                        .withReplicas(1)
                        .withBootstrapServers("my-kafka:9092")
                        .withAffinity(affinity)
                    .endSpec()
                    .build();

            kafkaOp.inNamespace(NAMESPACE).create(kafka1);
            connectOp.inNamespace(NAMESPACE).create(connect1);

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
            assertThat(commandOutput, containsString("Converting KafkaConnectS2I resource named connect1 from namespace " + NAMESPACE));
            assertThat(commandOutput, containsString("KafkaConnectS2I resource named connect1 in namespace " + NAMESPACE + " has been converted"));

            Kafka actualKafka1 = kafkaOp.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

            KafkaConnectS2I actualConnect1 = connectOp.inNamespace(NAMESPACE).withName("connect1").get();
            assertThat(actualConnect1.getSpec().getAffinity(), is(nullValue()));
            assertThat(actualConnect1.getSpec().getTemplate().getPod().getAffinity(), is(affinity));

        } finally {
            kafkaOp.inNamespace(NAMESPACE).withName("kafka1").delete();
            connectOp.inNamespace(NAMESPACE).withName("connect1").delete();
        }
    }

    @Test
    public void tesAllNamespacesConversion() {
        String namespace2 = NAMESPACE + "2";
        CLUSTER.createNamespace(namespace2);
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
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

            op.inNamespace(NAMESPACE).create(kafka1);
            op.inNamespace(namespace2).create(kafka2);

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

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

            Kafka actualKafka2 = op.inNamespace(namespace2).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));

        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(namespace2).withName("kafka2").delete();
        }
    }

    @Test
    public void testWrongNamespaceOrKindConversion() {
        CLUSTER.createNamespace(NAMESPACE + "-other");
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = LegacyCrds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withKafkaListeners(new KafkaListenersBuilder()
                                        .withNewPlain()
                                        .endPlain()
                                        .withNewTls()
                                        .endTls()
                                        .withNewKafkaListenerExternalLoadBalancer()
                                        .endKafkaListenerExternalLoadBalancer()
                                        .build())
                            .endListeners()
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

            op.inNamespace(NAMESPACE).create(kafka1);
            op.inNamespace(NAMESPACE).create(kafka2);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--kind", "Kafka", "--namespace", NAMESPACE + "-other");
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted")));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka2 in namespace " + NAMESPACE + " has been converted")));

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));

            Kafka actualKafka2 = op.inNamespace(NAMESPACE).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));

            exitCode = cmd.execute("convert-resource", "--kind", "Kafka");
            assertThat(exitCode, is(0));

            commandOutput = sw.toString();
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted")));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka2 in namespace " + NAMESPACE + " has been converted")));

            actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));

            actualKafka2 = op.inNamespace(NAMESPACE).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));

            exitCode = cmd.execute("convert-resource", "--kind", "KafkaUser", "--namespace", NAMESPACE);
            assertThat(exitCode, is(0));

            commandOutput = sw.toString();
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka1 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka1 in namespace " + NAMESPACE + " has been converted")));
            assertThat(commandOutput, not(containsString("Converting Kafka resource named kafka2 from namespace " + NAMESPACE)));
            assertThat(commandOutput, not(containsString("Kafka resource named kafka2 in namespace " + NAMESPACE + " has been converted")));

            actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));

            actualKafka2 = op.inNamespace(NAMESPACE).withName("kafka2").get();
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka2.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));

        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
            op.inNamespace(NAMESPACE).withName("kafka2").delete();
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

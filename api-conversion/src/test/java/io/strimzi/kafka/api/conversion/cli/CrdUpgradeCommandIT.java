/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class CrdUpgradeCommandIT {
    private static final String NAMESPACE = "api-conversion";
    private static final KubeClusterResource CLUSTER = KubeClusterResource.getInstance();
    private final KubernetesClient client = new DefaultKubernetesClient();

    @BeforeEach
    void setupEnvironment() {
        CLUSTER.cluster();
        CLUSTER.createNamespace(NAMESPACE);
        CliTestUtils.setupAllCrds(CLUSTER);

        // Checks that the old CRDs are really deployed as expected => v1beta1 or v1alpha1 are the stored versions
        CliTestUtils.crdHasTheExpectedInitialState(client);
    }

    @AfterEach
    void teardownEnvironment() {
        CliTestUtils.deleteAllCrds(CLUSTER);
        CLUSTER.deleteNamespaces();
    }

    /**
     * Tests the upgrade of CRDs (to use v1beta2 as the stored version) after all custom resources have been converted
     * and are compatible with v1beta2.
     */
    @Test
    public void testCrdUpgradeWithConvertedResources() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = Crds.kafkaOperation(client, ApiVersion.V1BETA2.toString());

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
                            .withNewJmxPrometheusExporterMetricsConfig()
                                .withNewValueFrom()
                                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                                            .withKey("zoo-metrics")
                                            .withName("metrics-cm")
                                            .withOptional(false)
                                            .build())
                                .endValueFrom()
                            .endJmxPrometheusExporterMetricsConfig()
                        .endZookeeper()
                        .withNewKafka()
                            .withVersion("2.7.0")
                            .withReplicas(3)
                            .withNewListeners()
                                .withGenericKafkaListeners(
                                        new GenericKafkaListenerBuilder()
                                                .withName("plain")
                                                .withPort(9092)
                                                .withTls(false)
                                                .withType(KafkaListenerType.INTERNAL)
                                                .build(),
                                        new GenericKafkaListenerBuilder()
                                                .withName("tls")
                                                .withPort(9093)
                                                .withTls(true)
                                                .withType(KafkaListenerType.INTERNAL)
                                                .build())
                            .endListeners()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                            .withNewJmxPrometheusExporterMetricsConfig()
                                .withNewValueFrom()
                                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                                            .withKey("kafka-metrics")
                                            .withName("metrics-cm")
                                            .withOptional(false)
                                            .build())
                                .endValueFrom()
                            .endJmxPrometheusExporterMetricsConfig()
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

            int exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Updating all Strimzi CRs to be stored under v1beta2"));
            assertThat(commandOutput, containsString("Updating Kafka kafka1 to be stored as v1beta2"));
            assertThat(commandOutput, containsString("Changing stored version in all Strimzi CRDs to v1beta2:"));
            assertThat(commandOutput, containsString("Changing stored version in statuses of all Strimzi CRDs to v1beta2:"));
            assertThat(commandOutput, containsString("Updating Kafka CRD"));
            assertThat(commandOutput, containsString("Updating KafkaMirrorMaker CRD"));
            assertThat(commandOutput, containsString("Updating KafkaRebalance CRD"));
            assertThat(commandOutput, containsString("Updating KafkaUser CRD"));
            assertThat(commandOutput, containsString("Updating KafkaMirrorMaker2 CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnect CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnectS2I CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnector CRD"));
            assertThat(commandOutput, containsString("Updating KafkaTopic CRD"));
            assertThat(commandOutput, containsString("Updating KafkaBridge CRD"));

            CliTestUtils.crdSpecHasUpdatedStorage(client);
            CliTestUtils.crdStatusHasUpdatedStorageVersions(client);

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1, is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(2));
            assertThat(actualKafka1.getSpec().getKafka().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
        }
    }

    /**
     * Tests the upgrade of CRDs (to use v1beta2 as the stored version) without all resources being converted and
     * compatible with v1beta2. This should cause a failure of the crd-upgrade command because it cannot store all CRs
     * as v1beta2.
     */
    @Test
    public void testUpgradeWithUnconvertedResourcesFails() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> op = Crds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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
                            .withMetrics(Map.of("somekey1", "somevalue1", "somekey2", "somevalue2"))
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
                            .withMetrics(Map.of("somekey3", "somevalue3", "somekey4", "somevalue4"))
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

            int exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(1));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Updating all Strimzi CRs to be stored under v1beta2"));
            assertThat(commandOutput, containsString("Updating Kafka kafka1 to be stored as v1beta2"));
            assertThat(commandOutput, containsString("Changing stored version in all Strimzi CRDs to v1beta2:"));
            assertThat(commandOutput, containsString("Updating Kafka CRD"));
            assertThat(commandOutput, containsString("Updating KafkaMirrorMaker CRD"));
            assertThat(commandOutput, containsString("Updating KafkaRebalance CRD"));
            assertThat(commandOutput, containsString("Updating KafkaUser CRD"));
            assertThat(commandOutput, containsString("Updating KafkaMirrorMaker2 CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnect CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnectS2I CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnector CRD"));
            assertThat(commandOutput, containsString("Updating KafkaTopic CRD"));
            assertThat(commandOutput, containsString("Updating KafkaBridge CRD"));

            CliTestUtils.crdSpecHasUpdatedStorage(client);
            CliTestUtils.crdStatusHasNotUpdatedStorageVersions(client);

            Kafka actualKafka1 = op.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1, is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetrics(), is(Map.of("somekey3", "somevalue3", "somekey4", "somevalue4")));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetrics(), is(Map.of("somekey1", "somevalue1", "somekey2", "somevalue2")));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig(), is(nullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
        }
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

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
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class FullUpgradeLifecycleIT {
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
     * Tests the full lifecycle of the CRD upgrade. It:
     *     1) Starts with the initial CRDs containing all versions
     *     2) Creates the resources with the old versions and APIs
     *     3) Converts them using convert-resources subcommand
     *     4) Upgrades the CRDs to use v1beta2 as stored version using the crd-upgrade subcommand
     *     5) Installs the CRD v1 definitions containing only the v1beta2 version
     *     6) Checks that the KAfka CR is still there and looks good
     *
     * This test checks the whole lifecycle as the user should run it.
     */
    @Test
    public void testFullLifecycleAsExpected() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> opV1Beta2 = Crds.kafkaOperation(client, ApiVersion.V1BETA2.toString());
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> opV1Beta1 = Crds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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

            opV1Beta1.inNamespace(NAMESPACE).create(kafka1);

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            // Convert resources
            int exitCode = cmd.execute("convert-resources", "--all-namespaces");
            assertThat(exitCode, is(0));

            // Upgrade CRDs
            exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(0));

            CliTestUtils.crdSpecHasUpdatedStorage(client);
            CliTestUtils.crdStatusHasUpdatedStorageVersions(client);

            // Install CRD v1 with v1beta2 only
            CliTestUtils.setupV1Crds(CLUSTER);
            CliTestUtils.crdHasV1Beta2Only(client);

            Kafka actualKafka1 = opV1Beta2.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1, is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));
            assertThat(actualKafka1.getSpec().getKafka().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
        } finally {
            opV1Beta2.inNamespace(NAMESPACE).withName("kafka1").delete();
            CliTestUtils.deleteV1Crds(CLUSTER);
        }
    }

    /**
     * Tests the full lifecycle of the CRD upgrade when user forgets to convert all custom resources. It:
     *     1) Starts with the initial CRDs containing all versions
     *     2) Creates the resources with the old versions and APIs
     *     3) Upgrades the CRDs to use v1beta2 as stored version using the crd-upgrade subcommand => this fails because they were not converted first
     *     4) Converts them using convert-resources subcommand
     *     5) Upgrades the CRDs to use v1beta2 as stored version using the crd-upgrade subcommand => this fails because they were not converted first
     *     6) Installs the CRD v1 definitions containing only the v1beta2 version
     *     7) Checks that the KAfka CR is still there and looks good
     *
     * This test checks the whole lifecycle but expects that the user forgot to convert something and the crd-upgrade
     * step fails. So the user needs to go back, convert it and then run the CRD upgrade again.
     */
    @Test
    public void testFullLifecycleOnSecondTry() {
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> opV1Beta2 = Crds.kafkaOperation(client, ApiVersion.V1BETA2.toString());
        MixedOperation<Kafka, KafkaList, Resource<Kafka>> opV1Beta1 = Crds.kafkaOperation(client, ApiVersion.V1BETA1.toString());

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

            opV1Beta1.inNamespace(NAMESPACE).create(kafka1);

            // First try with unconverted resources => should fail
            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(1));

            CliTestUtils.crdSpecHasUpdatedStorage(client);
            CliTestUtils.crdStatusHasNotUpdatedStorageVersions(client);

            // Convert resources
            exitCode = cmd.execute("convert-resources", "--all-namespaces");
            assertThat(exitCode, is(0));

            // Upgrade CRDs
            exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(0));

            CliTestUtils.crdSpecHasUpdatedStorage(client);
            CliTestUtils.crdStatusHasUpdatedStorageVersions(client);

            //Install CRDs v1 with v1beta2 only
            CliTestUtils.setupV1Crds(CLUSTER);
            CliTestUtils.crdHasV1Beta2Only(client);

            Kafka actualKafka1 = opV1Beta2.inNamespace(NAMESPACE).withName("kafka1").get();
            assertThat(actualKafka1, is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(3));
            assertThat(actualKafka1.getSpec().getKafka().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetrics(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig(), is(notNullValue()));
            assertThat(actualKafka1.getSpec().getZookeeper().getMetricsConfig().getType(), is(JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER));
        } finally {
            opV1Beta2.inNamespace(NAMESPACE).withName("kafka1").delete();
            CliTestUtils.deleteV1Crds(CLUSTER);
        }
    }
}

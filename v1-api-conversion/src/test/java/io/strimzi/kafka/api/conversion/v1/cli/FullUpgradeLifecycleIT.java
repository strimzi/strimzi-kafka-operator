/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.test.TestUtils;
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

@SuppressWarnings("deprecation")
public class FullUpgradeLifecycleIT {
    private static final String NAMESPACE = "api-conversion";

    private KubernetesClient client;

    @BeforeEach
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        ConversionTestUtils.createV1beta2Crds(client);
        TestUtils.createNamespace(client, NAMESPACE);

        // Checks that the old CRDs are really deployed as expected => v1beta1 or v1alpha1 are the stored versions
        ConversionTestUtils.crdHasTheExpectedInitialState(client);
    }

    @AfterEach
    void teardownEnvironment() {
        ConversionTestUtils.deleteAllCrds(client);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }

    /**
     * Tests the full lifecycle of the CRD upgrade. It:
     *     1) Starts with the initial CRDs containing all versions
     *     2) Creates the resources with the old versions and APIs
     *     3) Converts them using convert-resources subcommand
     *     4) Upgrades the CRDs to use v1beta2 as stored version using the crd-upgrade subcommand
     *     5) Installs the CRD v1 definitions containing only the v1beta2 version
     *     6) Checks that the Kafka CR is still there and looks good
     *
     * This test checks the whole lifecycle as the user should run it.
     */
    @Test
    public void testFullLifecycleAsExpected() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> opV1 = ConversionTestUtils.kafkaV1Operation(client);

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

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(kafka1)).create();

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

            ConversionTestUtils.crdSpecHasUpdatedStorage(client);
            ConversionTestUtils.crdStatusHasUpdatedStorageVersions(client);

            // Install CRD v1 with v1beta2 only
            ConversionTestUtils.createV1Crds(client);
            ConversionTestUtils.crdHasV1Only(client);

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(opV1.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));
        } finally {
            opV1.inNamespace(NAMESPACE).withName("kafka1").delete();
            ConversionTestUtils.deleteAllCrds(client);
        }
    }

    /**
     * Tests the full lifecycle of the CRD upgrade when user forgets to convert all custom resources. It:
     *     1) Starts with the initial CRDs containing all versions
     *     2) Creates the resources with the old versions and APIs
     *     3) Upgrades the CRDs to use v1 as stored version using the crd-upgrade subcommand => this fails because they were not converted first
     *     4) Converts them using convert-resources subcommand
     *     5) Upgrades the CRDs to use v1 as stored version using the crd-upgrade subcommand => this fails because they were not converted first
     *     6) Installs the CRD v1 definitions containing only the v1 version
     *     7) Checks that the KafkaConnect CR is still there and looks good
     *
     * This test checks the whole lifecycle but expects that the user forgot to convert something and the crd-upgrade
     * step fails. So the user needs to go back, convert it and then run the CRD upgrade again.
     */
    @Test
    public void testFullLifecycleOnSecondTry() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.connectOperation(client);
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> opV1 = ConversionTestUtils.connectV1Operation(client);

        try {
            KafkaConnect connect1 = new KafkaConnectBuilder()
                    .withNewMetadata()
                        .withName("connect1")
                    .endMetadata()
                    .withNewSpec()
                        .withBootstrapServers("localhost:9092")
                        .withConfig(Map.of("group.id", "my-group",
                                "offset.storage.topic", "my-offsets",
                                "config.storage.topic", "my-configs",
                                "status.storage.topic", "my-statuses"))
                    .endSpec()
                    .build();

            op.inNamespace(NAMESPACE).resource(ConversionTestUtils.typedToGeneric(connect1)).create();

            // First try with unconverted resources => should fail
            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(1));

            ConversionTestUtils.crdSpecHasUpdatedStorage(client);
            ConversionTestUtils.crdStatusHasNotUpdatedStorageVersions(client);

            // Convert resources
            exitCode = cmd.execute("convert-resources", "--all-namespaces");
            assertThat(exitCode, is(0));

            // Upgrade CRDs
            exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(0));

            ConversionTestUtils.crdSpecHasUpdatedStorage(client);
            ConversionTestUtils.crdStatusHasUpdatedStorageVersions(client);

            //Install CRDs v1 with v1beta2 only
            ConversionTestUtils.createV1Crds(client);
            ConversionTestUtils.crdHasV1Only(client);

            KafkaConnect actualConnect1 = ConversionTestUtils.genericToTyped(opV1.inNamespace(NAMESPACE).withName("connect1").get(), KafkaConnect.class);
            assertThat(actualConnect1, is(notNullValue()));
            assertThat(actualConnect1.getSpec().getReplicas(), is(3));
            assertThat(actualConnect1.getSpec().getGroupId(), is("my-group"));
            assertThat(actualConnect1.getSpec().getConfigStorageTopic(), is("my-configs"));
            assertThat(actualConnect1.getSpec().getOffsetStorageTopic(), is("my-offsets"));
            assertThat(actualConnect1.getSpec().getStatusStorageTopic(), is("my-statuses"));
            assertThat(actualConnect1.getSpec().getConfig().get("group.id"), is(nullValue()));
            assertThat(actualConnect1.getSpec().getConfig().get("config.storage.topic"), is(nullValue()));
            assertThat(actualConnect1.getSpec().getConfig().get("offset.storage.topic"), is(nullValue()));
            assertThat(actualConnect1.getSpec().getConfig().get("status.storage.topic"), is(nullValue()));
        } finally {
            opV1.inNamespace(NAMESPACE).withName("connect1").delete();
            ConversionTestUtils.deleteAllCrds(client);
        }
    }
}

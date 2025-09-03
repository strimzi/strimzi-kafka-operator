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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
public class CrdUpgradeCommandIT {
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
     * Tests the upgrade of CRDs (to use v1beta2 as the stored version) after all custom resources have been converted
     * and are compatible with v1beta2.
     */
    @Test
    public void testCrdUpgradeWithConvertedResources() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.kafkaOperation(client);

        try {
            Kafka kafka1 = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("kafka1")
                    .endMetadata()
                    .withNewSpec()
                        .withNewKafka()
                            .withListeners(
                                    new GenericKafkaListenerBuilder()
                                            .withName("tls")
                                            .withPort(9093)
                                            .withType(KafkaListenerType.INTERNAL)
                                            .build()
                            )
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

            int exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(0));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Updating all Strimzi CRs to be stored under v1"));
            assertThat(commandOutput, containsString("Updating Kafka kafka1 to be stored as v1"));
            assertThat(commandOutput, containsString("Changing stored version in all Strimzi CRDs to v1:"));
            assertThat(commandOutput, containsString("Changing stored version in statuses of all Strimzi CRDs to v1:"));
            assertThat(commandOutput, containsString("Updating Kafka CRD"));
            assertThat(commandOutput, containsString("Updating StrimziPodSet CRD"));
            assertThat(commandOutput, containsString("Updating KafkaRebalance CRD"));
            assertThat(commandOutput, containsString("Updating KafkaUser CRD"));
            assertThat(commandOutput, containsString("Updating KafkaMirrorMaker2 CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnect CRD"));
            assertThat(commandOutput, containsString("Updating KafkaNodePool CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnector CRD"));
            assertThat(commandOutput, containsString("Updating KafkaTopic CRD"));
            assertThat(commandOutput, containsString("Updating KafkaBridge CRD"));

            ConversionTestUtils.crdSpecHasUpdatedStorage(client);
            ConversionTestUtils.crdStatusHasUpdatedStorageVersions(client);

            Kafka actualKafka1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("kafka1").get(), Kafka.class);
            assertThat(actualKafka1.getSpec().getZookeeper(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getStorage(), is(nullValue()));
            assertThat(actualKafka1.getSpec().getKafka().getReplicas(), is(nullValue()));
        } finally {
            op.inNamespace(NAMESPACE).withName("kafka1").delete();
        }
    }

    /**
     * Tests the upgrade of CRDs (to use v1beta2 as the stored version) without all resources being converted and
     * compatible with v1beta2. This should cause a failure of the crd-upgrade command because it cannot store all CRs
     * as v1beta2.
     *
     * IMPORTANT: The crd-upgrade fails only if the resource is not valid v1 resource, for example due to missing required fields.
     */
    @Test
    public void testUpgradeWithUnconvertedResourcesFails() {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = ConversionTestUtils.connectOperation(client);

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

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("crd-upgrade");
            assertThat(exitCode, is(1));

            String commandOutput = sw.toString();
            assertThat(commandOutput, containsString("Updating all Strimzi CRs to be stored under v1"));
            assertThat(commandOutput, containsString("Updating KafkaConnect connect1 to be stored as v1"));
            assertThat(commandOutput, containsString("Changing stored version in all Strimzi CRDs to v1:"));
            assertThat(commandOutput, containsString("Updating Kafka CRD"));
            assertThat(commandOutput, containsString("Updating StrimziPodSet CRD"));
            assertThat(commandOutput, containsString("Updating KafkaRebalance CRD"));
            assertThat(commandOutput, containsString("Updating KafkaUser CRD"));
            assertThat(commandOutput, containsString("Updating KafkaMirrorMaker2 CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnect CRD"));
            assertThat(commandOutput, containsString("Updating KafkaNodePool CRD"));
            assertThat(commandOutput, containsString("Updating KafkaConnector CRD"));
            assertThat(commandOutput, containsString("Updating KafkaTopic CRD"));
            assertThat(commandOutput, containsString("Updating KafkaBridge CRD"));

            ConversionTestUtils.crdSpecHasUpdatedStorage(client);
            ConversionTestUtils.crdStatusHasNotUpdatedStorageVersions(client);

            KafkaConnect actualConnect1 = ConversionTestUtils.genericToTyped(op.inNamespace(NAMESPACE).withName("connect1").get(), KafkaConnect.class);
            assertThat(actualConnect1, is(notNullValue()));
            assertThat(actualConnect1.getSpec().getReplicas(), is(3));
            assertThat(actualConnect1.getSpec().getGroupId(), is(nullValue()));
            assertThat(actualConnect1.getSpec().getConfigStorageTopic(), is(nullValue()));
            assertThat(actualConnect1.getSpec().getOffsetStorageTopic(), is(nullValue()));
            assertThat(actualConnect1.getSpec().getStatusStorageTopic(), is(nullValue()));
            assertThat(actualConnect1.getSpec().getConfig().get("group.id"), is("my-group"));
            assertThat(actualConnect1.getSpec().getConfig().get("config.storage.topic"), is("my-configs"));
            assertThat(actualConnect1.getSpec().getConfig().get("offset.storage.topic"), is("my-offsets"));
            assertThat(actualConnect1.getSpec().getConfig().get("status.storage.topic"), is("my-statuses"));
        } finally {
            op.inNamespace(NAMESPACE).withName("connect1").delete();
        }
    }
}

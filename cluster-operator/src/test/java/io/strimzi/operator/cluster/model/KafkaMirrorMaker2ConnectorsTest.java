/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.operator.cluster.model.KafkaMirrorMaker2Connectors.PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaMirrorMaker2ConnectorsTest {
    private static final String PREFIX = "prefix.";

    private final static KafkaMirrorMaker2 KMM2 = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName("my-mm2")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewTarget()
                    .withAlias("target")
                    .withGroupId("my-mm2-group")
                    .withConfigStorageTopic("my-mm2-config")
                    .withOffsetStorageTopic("my-mm2-offset")
                    .withStatusStorageTopic("my-mm2-status")
                    .withBootstrapServers("target:9092")
                .endTarget()
                .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withNewSource()
                            .withAlias("source")
                            .withBootstrapServers("source:9092")
                        .endSource()
                        .withNewSourceConnector()
                            .withTasksMax(5)
                            .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                        .endSourceConnector()
                        .withNewCheckpointConnector()
                            .withTasksMax(3)
                            .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                        .endCheckpointConnector()
                        .withTopicsPattern("my-topic-.*")
                        .withTopicsExcludePattern("exclude-topic-.*")
                        .withGroupsPattern("my-group-.*")
                        .withGroupsExcludePattern("exclude-group-.*")
                        .build())
            .endSpec()
            .build();

    @Test
    public void testConnectors() {
        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KMM2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        Map<String, Object> expectedAll = new TreeMap<>();
        expectedAll.put("source.cluster.alias", "source");
        expectedAll.put("source.cluster.bootstrap.servers", "source:9092");
        expectedAll.put("source.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("target.cluster.alias", "target");
        expectedAll.put("target.cluster.bootstrap.servers", "target:9092");
        expectedAll.put("target.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("topics", "my-topic-.*");
        expectedAll.put("topics.exclude", "exclude-topic-.*");
        expectedAll.put("groups", "my-group-.*");
        expectedAll.put("groups.exclude", "exclude-group-.*");

        Map<String, Object> expectedSource = new TreeMap<>(expectedAll);
        expectedSource.put("sync.topic.acls.enabled", "false");

        Map<String, Object> expectedCheckpoint = new TreeMap<>(expectedAll);
        expectedCheckpoint.put("sync.group.offsets.enabled", "true");

        assertThat(kcs.size(), is(2));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(5));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(3));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedCheckpoint));
    }

    @Test
    public void testOverridingSourceAndTargetConfiguration() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withNewSource()
                            .withAlias("source")
                            .withBootstrapServers("source:9092")
                        .endSource()
                        .withNewSourceConnector()
                            .withConfig(Map.of(
                                    "target.cluster.security.protocol", "XXX",
                                    "source.cluster.security.protocol", "YYY"
                            ))
                        .endSourceConnector()
                        .build())
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        Map<String, Object> expectedSource = new TreeMap<>();
        expectedSource.put("target.cluster.alias", "target");
        expectedSource.put("target.cluster.bootstrap.servers", "target:9092");
        expectedSource.put("target.cluster.security.protocol", "XXX");
        expectedSource.put("source.cluster.alias", "source");
        expectedSource.put("source.cluster.bootstrap.servers", "source:9092");
        expectedSource.put("source.cluster.security.protocol", "YYY");

        assertThat(kcs.size(), is(1));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource));
    }

    @Test
    public void testConnectorsWithMultipleSources() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("other-source")
                                .withBootstrapServers("other-source:9092")
                            .endSource()
                            .withNewSourceConnector()
                                .withTasksMax(15)
                                .withConfig(Map.of("sync.topic.acls.enabled", "true"))
                            .endSourceConnector()
                            .withNewCheckpointConnector()
                                .withTasksMax(13)
                                .withConfig(Map.of("sync.group.offsets.enabled", "false"))
                            .endCheckpointConnector()
                            .build())
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        Map<String, Object> expectedAll = new TreeMap<>();
        expectedAll.put("source.cluster.alias", "source");
        expectedAll.put("source.cluster.bootstrap.servers", "source:9092");
        expectedAll.put("source.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("target.cluster.alias", "target");
        expectedAll.put("target.cluster.bootstrap.servers", "target:9092");
        expectedAll.put("target.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("topics", "my-topic-.*");
        expectedAll.put("topics.exclude", "exclude-topic-.*");
        expectedAll.put("groups", "my-group-.*");
        expectedAll.put("groups.exclude", "exclude-group-.*");

        Map<String, Object> expectedSource = new TreeMap<>(expectedAll);
        expectedSource.put("sync.topic.acls.enabled", "false");

        Map<String, Object> expectedCheckpoint = new TreeMap<>(expectedAll);
        expectedCheckpoint.put("sync.group.offsets.enabled", "true");

        Map<String, Object> expectedAll2 = new TreeMap<>();
        expectedAll2.put("source.cluster.alias", "other-source");
        expectedAll2.put("source.cluster.bootstrap.servers", "other-source:9092");
        expectedAll2.put("source.cluster.security.protocol", "PLAINTEXT");
        expectedAll2.put("target.cluster.alias", "target");
        expectedAll2.put("target.cluster.bootstrap.servers", "target:9092");
        expectedAll2.put("target.cluster.security.protocol", "PLAINTEXT");

        Map<String, Object> expectedSource2 = new TreeMap<>(expectedAll2);
        expectedSource2.put("sync.topic.acls.enabled", "true");

        Map<String, Object> expectedCheckpoint2 = new TreeMap<>(expectedAll2);
        expectedCheckpoint2.put("sync.group.offsets.enabled", "false");

        assertThat(kcs.size(), is(4));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(5));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(3));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedCheckpoint));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("other-source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("other-source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(15));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource2));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("other-source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("other-source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(13));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedCheckpoint2));
    }

    @Test
    public void testConnectorsOnlySome() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .editMirror(0)
                        .withCheckpointConnector(null)
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        assertThat(kcs.size(), is(1));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
    }

    @Test
    public void testConnectorsWithAutoRestart() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                                .withMaxRestarts(1874)
                            .endAutoRestart()
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .withNewAutoRestart()
                                .withEnabled(false)
                            .endAutoRestart()
                        .endCheckpointConnector()
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        assertThat(kcs.size(), is(2));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getAutoRestart(), is(notNullValue()));
        assertThat(kc.getSpec().getAutoRestart().isEnabled(), is(true));
        assertThat(kc.getSpec().getAutoRestart().getMaxRestarts(), is(1874));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getAutoRestart(), is(notNullValue()));
        assertThat(kc.getSpec().getAutoRestart().isEnabled(), is(false));
    }

    @Test
    public void testConnectorConfiguration() {
        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KMM2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(KMM2.getSpec().getMirrors().get(0),
                KMM2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testConnectorConfigurationWithTracing() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withNewOpenTelemetryTracing()
                    .endOpenTelemetryTracing()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(KMM2.getSpec().getMirrors().get(0),
                KMM2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");
        expected.put("consumer.interceptor.classes", "io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor");
        expected.put("producer.interceptor.classes", "io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testConnectorConfigurationWithTopologyLabelRack() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withNewTopologyLabelRack()
                        .withTopologyKey("my-topology-key")
                    .endTopologyLabelRack()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(KMM2.getSpec().getMirrors().get(0),
                KMM2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");
        expected.put("consumer.client.rack", "${strimzifile:/tmp/strimzi-connect.properties:consumer.client.rack}");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testConnectorConfigurationWithEnvironmentVariableRack() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withNewEnvironmentVariableRack()
                        .withEnvVarName("MY_RACK_ID")
                    .endEnvironmentVariableRack()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(KMM2.getSpec().getMirrors().get(0),
                KMM2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");
        expected.put("consumer.client.rack", "${strimzifile:/tmp/strimzi-connect.properties:consumer.client.rack}");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithoutAuth() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                                "prefix.security.protocol", "PLAINTEXT",
                                "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithoutAuthWithClusterConfig() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withConfig(Map.of("config.storage.replication.factor", "-1"))
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                                "prefix.security.protocol", "PLAINTEXT",
                                "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092",
                                "prefix.config.storage.replication.factor", "-1"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithTlsAuth() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName("my-secret")
                        .withCertificate("my.crt")
                        .withKey("my.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                .endTls()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        assertThat(config.containsKey("prefix.sasl.jaas.config"), is(false));
        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                                "prefix.security.protocol", "SSL",
                                "prefix.ssl.keystore.location", "/tmp/kafka/clusters/sourceClusterAlias.keystore.p12",
                                "prefix.ssl.keystore.password", PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR,
                                "prefix.ssl.keystore.type", "PKCS12",
                                "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithPlain() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("shaza")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("pa55word")
                    .endPasswordSecret()
                    .endKafkaClientAuthenticationPlain()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat(configEntry.getLoginModuleName(), is("org.apache.kafka.common.security.plain.PlainLoginModule"));
        assertThat(configEntry.getOptions(),
                is(Map.of("username", "shaza",
                        "password", "${strimzidir:/opt/kafka/mm2-password/sourceClusterAlias/my-secret:pa55word}")));

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.security.protocol", "SASL_PLAINTEXT",
                        "prefix.sasl.mechanism", "PLAIN",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithScram() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("shaza")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("pa55word")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat(configEntry.getLoginModuleName(), is("org.apache.kafka.common.security.scram.ScramLoginModule"));
        assertThat(configEntry.getOptions(),
                is(Map.of("username", "shaza",
                        "password", "${strimzidir:/opt/kafka/mm2-password/sourceClusterAlias/my-secret:pa55word}")));

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.security.protocol", "SASL_PLAINTEXT",
                        "prefix.sasl.mechanism", "SCRAM-SHA-512",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithScramAndTlsEncryption() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("shaza")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("pa55word")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-tls").withCertificate("ca.crt").build())
                .endTls()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat("org.apache.kafka.common.security.scram.ScramLoginModule", is(configEntry.getLoginModuleName()));
        assertThat(configEntry.getOptions(),
                is(Map.of("username", "shaza",
                        "password", "${strimzidir:/opt/kafka/mm2-password/sourceClusterAlias/my-secret:pa55word}")));

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.security.protocol", "SASL_SSL",
                       "prefix.ssl.truststore.location", "/tmp/kafka/clusters/sourceClusterAlias.truststore.p12",
                       "prefix.ssl.truststore.password", PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR,
                        "prefix.ssl.truststore.type", "PKCS12",
                        "prefix.sasl.mechanism", "SCRAM-SHA-512",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithCustomAuthenticationWithoutSasl() {
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(false)
                    .withConfig(Map.of("ssl.keystore.location", "/mnt/certs/keystore", "ssl.keystore.password", "changeme", "not.allowed", "foo"))
                .endKafkaClientAuthenticationCustom()
                .build();

        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        assertThat(config.size(), is(5));
        assertThat(config.get("prefix.bootstrap.servers"), is("sourceClusterAlias.sourceNamespace.svc:9092"));
        assertThat(config.get("prefix.alias"), is("sourceClusterAlias"));
        assertThat(config.get("prefix.security.protocol"), is("PLAINTEXT"));
        assertThat(config.get("prefix.ssl.keystore.location"), is("/mnt/certs/keystore"));
        assertThat(config.get("prefix.ssl.keystore.password"), is("changeme"));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithCustomAuthenticationWithSasl() {
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(true)
                    .withConfig(Map.of("sasl.mechanism", "AWS_MSK_IAM", "sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;", "sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler", "not.allowed", "foo"))
                .endKafkaClientAuthenticationCustom()
                .build();

        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, config, cluster, PREFIX);

        assertThat(config.size(), is(6));
        assertThat(config.get("prefix.bootstrap.servers"), is("sourceClusterAlias.sourceNamespace.svc:9092"));
        assertThat(config.get("prefix.alias"), is("sourceClusterAlias"));
        assertThat(config.get("prefix.security.protocol"), is("SASL_PLAINTEXT"));
        assertThat(config.get("prefix.sasl.mechanism"), is("AWS_MSK_IAM"));
        assertThat(config.get("prefix.sasl.jaas.config"), is("software.amazon.msk.auth.iam.IAMLoginModule required;"));
        assertThat(config.get("prefix.sasl.client.callback.handler.class"), is("software.amazon.msk.auth.iam.IAMClientCallbackHandler"));
    }

    @Test
    public void testStrimziMetricsReporter() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("source")
                                .withBootstrapServers("source:9092")
                            .endSource()
                            .withNewSourceConnector()
                            .endSourceConnector()
                            .build())
                    .withMetricsConfig(new StrimziMetricsReporterBuilder().build())
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(kmm2.getSpec().getMirrors().get(0), kmm2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("metric.reporters", StrimziMetricsReporterConfig.CLIENT_CLASS);
        expected.put(StrimziMetricsReporterConfig.LISTENER_ENABLE, "false");
        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testStrimziAndCustomMetricsReporters() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("source")
                                .withBootstrapServers("source:9092")
                            .endSource()
                            .withNewSourceConnector()
                                .addToConfig(Map.of("metric.reporters", "com.example.ExistingReporter"))
                            .endSourceConnector()
                            .build())
                    .withMetricsConfig(new StrimziMetricsReporterBuilder().build())
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(kmm2.getSpec().getMirrors().get(0), kmm2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("metric.reporters", "com.example.ExistingReporter," + StrimziMetricsReporterConfig.CLIENT_CLASS);
        expected.put(StrimziMetricsReporterConfig.LISTENER_ENABLE, "false");
        assertThat(new TreeMap<>(config), is(expected));
    }
    
    @Test
    public void testStrimziMetricsReporterViaUserAndMetricsConfigs() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("source")
                                .withBootstrapServers("source:9092")
                            .endSource()
                            .withNewSourceConnector()
                                .addToConfig(Map.of("metric.reporters", StrimziMetricsReporterConfig.CLIENT_CLASS))
                            .endSourceConnector()
                            .build())
                    .withMetricsConfig(new StrimziMetricsReporterBuilder().build())
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(kmm2.getSpec().getMirrors().get(0), kmm2.getSpec().getMirrors().get(0).getSourceConnector());

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("metric.reporters", StrimziMetricsReporterConfig.CLIENT_CLASS);
        expected.put(StrimziMetricsReporterConfig.LISTENER_ENABLE, "false");
        assertThat(new TreeMap<>(config), is(expected));
    }
}

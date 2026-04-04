/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.EnvironmentVariableRackBuilder;
import io.strimzi.api.kafka.model.common.TopologyLabelRackBuilder;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorization;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafkaBuilder;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginStrimzi;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginStrimziBuilder;
import io.strimzi.api.kafka.model.kafka.tieredstorage.RemoteStorageManager;
import io.strimzi.api.kafka.model.kafka.tieredstorage.TieredStorageCustom;
import io.strimzi.operator.cluster.model.cruisecontrol.CruiseControlMetricsReporter;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.strimzi.operator.cluster.TestUtils.IsEquivalent.isEquivalent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class KafkaBrokerConfigurationBuilderTest {
    private final static NodeRef NODE_REF = new NodeRef("my-cluster-kafka-2", 2, "kafka", false, true);

    @Test
    public void testBrokerId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .build();
        // brokers don't have broker.id when in KRaft-mode, only node.id
        assertThat(configuration, not(containsString("broker.id")));
        assertThat(configuration, containsString("node.id=2"));

        NodeRef controller = new NodeRef("my-cluster-kafka-3", 3, "kafka", true, false);
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, controller)
                .build();
        // controllers don't have broker.id at all, only node.id
        assertThat(configuration, not(containsString("broker.id")));
        assertThat(configuration, containsString("node.id=3"));
    }

    @Test
    public void testKraftMixedNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-kafka-0", 0, "kafka", true, true),
                new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true),
                new NodeRef("my-cluster-kafka-2", 2, "kafka", true, true)
        );

        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090"));
    }

    @Test
    public void testKraftControllerAndBrokerNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
                new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
                new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
                new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
                new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
                new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true)
        );

        // Controller-only node
        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090"));

        nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 11).findFirst().get();
        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .build();

        assertThat(configuration, isEquivalent("node.id=11",
                "process.roles=broker",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090"));
    }

    @Test
    public void testNoCruiseControl()  {
        // Broker configuration
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", null, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));

        // Controller configuration
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", null, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @Test
    public void testCruiseControl()  {
        CruiseControlMetricsReporter ccMetricsReporter = new CruiseControlMetricsReporter("strimzi.cruisecontrol.metrics", 1, 1, 1);

        // Broker configuration
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=strimzi.cruisecontrol.metrics",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO + "=HTTPS",
                CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS + "=my-cluster-kafka-brokers:9091",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL + "=SSL",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE + "=PEM",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_CERTIFICATE_CHAIN + "=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_KEY + "=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE + "=PEM",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_CERTIFICATES + "=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE + "=true",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=1",
                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=1",
                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=1"));

        // Controller configuration
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @Test
    public void testCruiseControlCustomMetricReporterTopic()  {
        CruiseControlMetricsReporter ccMetricsReporter = new CruiseControlMetricsReporter("metric-reporter-topic", 2, 3, 4);

        // Broker configuration
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=metric-reporter-topic",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO + "=HTTPS",
                CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS + "=my-cluster-kafka-brokers:9091",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL + "=SSL",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE + "=PEM",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_CERTIFICATE_CHAIN + "=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_KEY + "=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE + "=PEM",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_CERTIFICATES + "=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE + "=true",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=2",
                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=3",
                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=4"));

        // Controller configuration
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @Test
    public void testStrimziMetricsReporterEnabled()  {
        StrimziMetricsReporterModel model = new StrimziMetricsReporterModel(
                new KafkaClusterSpecBuilder()
                        .withMetricsConfig(new StrimziMetricsReporterBuilder()
                            .withNewValues()
                                .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                            .endValues()
                            .build())
                        .build(), List.of(".*"));

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withStrimziMetricsReporter(model)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true",
                StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT,
                StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_log.*,kafka_network.*"));
    }

    @Test
    public void testNoRackAwareness()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withRackId(null)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @Test
    public void testTopologyLabelRackIdInKRaftBrokers()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withRackId(new TopologyLabelRackBuilder().withTopologyKey("failure-domain.kubernetes.io/zone").build())
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "broker.rack=${strimzidir:/opt/kafka/init:rack.id}"));
    }

    @Test
    public void testTopologyLabelRackIdInKRaftMixedNode()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true))
                .withRackId(new TopologyLabelRackBuilder().withTopologyKey("failure-domain.kubernetes.io/zone").build())
                .build();

        assertThat(configuration, isEquivalent("node.id=1",
                "broker.rack=${strimzidir:/opt/kafka/init:rack.id}"));
    }

    @Test
    public void testTopologyLabelRackIdInKRaftControllers()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false))
                .withRackId(new TopologyLabelRackBuilder().withTopologyKey("failure-domain.kubernetes.io/zone").build())
                .build();

        assertThat(configuration, isEquivalent("node.id=1"));
    }

    @Test
    public void testEnvironmentVariableRackIdInKRaftBrokers()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withRackId(new EnvironmentVariableRackBuilder().withEnvVarName("MY_RACK_ID").build())
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "broker.rack=${strimzienv:MY_RACK_ID}"));
    }

    @Test
    public void testEnvironmentVariableRackIdInKRaftMixedNode()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true))
                .withRackId(new EnvironmentVariableRackBuilder().withEnvVarName("MY_RACK_ID").build())
                .build();

        assertThat(configuration, isEquivalent("node.id=1",
                "broker.rack=${strimzienv:MY_RACK_ID}"));
    }

    @Test
    public void testEnvironmentVariableRackIdInKRaftControllers()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false))
                .withRackId(new EnvironmentVariableRackBuilder().withEnvVarName("MY_RACK_ID").build())
                .build();

        assertThat(configuration, isEquivalent("node.id=1"));
    }

    @Test
    public void testNullAuthorization()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", null)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @Test
    public void testSimpleAuthorizationWithSuperUsers()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .addToSuperUsers("jakub", "CN=kuba")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jakub;User:CN=kuba"));
    }

    @Test
    public void testSimpleAuthorizationWithSuperUsersAndKRaft()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .addToSuperUsers("jakub", "CN=kuba")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jakub;User:CN=kuba"));
    }

    @Test
    public void testNullUserConfiguration()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, false, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1"));
    }

    @Test
    public void testNullUserConfigurationAndCCReporter()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, true, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter",
                "min.insync.replicas=1"));
    }

    @Test
    public void testEmptyUserConfiguration()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1"));
    }

    @Test
    public void testUserConfiguration()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("auto.create.topics.enable", "false");
        userConfiguration.put("offsets.topic.replication.factor", 3);
        userConfiguration.put("transaction.state.log.replication.factor", 3);
        userConfiguration.put("transaction.state.log.min.isr", 2);

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "auto.create.topics.enable=false",
                "offsets.topic.replication.factor=3",
                "transaction.state.log.replication.factor=3",
                "transaction.state.log.min.isr=2",
                "min.insync.replicas=1"));
    }

    @Test
    public void testUserConfigurationWithConfigProviders()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("config.providers", "env");
        userConfiguration.put("config.providers.env.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider");

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        // Broker
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=env,strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1"));

        // Controller
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-kafka-3", 3, "kafka", true, false))
                .withUserConfiguration(kafkaConfiguration, false, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=3",
                "config.providers=env,strimzienv,strimzisecrets",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1"));
    }
    
    @Test
    public void testUserConfigurationWithInvalidConfigProviders()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("config.providers", "env,strimzienv");
        userConfiguration.put("config.providers.env.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        userConfiguration.put("config.providers.strimzienv.class", "org.apache.kafka.common.config.provider.UserConfigProvider");

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        assertThrows(InvalidConfigurationException.class, () -> {
            new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false, false)
                .build();
        }, "InvalidConfigurationException was expected");
    }

    @Test
    public void testNullUserConfigurationWithJmxMetricsReporter()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, false, true, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "metric.reporters=org.apache.kafka.common.metrics.JmxReporter",
                "min.insync.replicas=1"));
    }

    @Test
    public void testNullUserConfigurationWithStrimziMetricsReporter() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, false, false, true)
                .build();
        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1",
                "metric.reporters=" + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS,
                "kafka.metrics.reporters=" + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS));
    }


    @Test
    public void testNullUserConfigurationWithCruiseControlAndStrimziMetricsReporters() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, true, false, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1",
                "metric.reporters=" + CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER
                        + "," + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS,
                "kafka.metrics.reporters=" + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS));
    }

    @Test
    public void testNullUserConfigurationWithCruiseControlAndJmxAndStrimziMetricsReporters() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, true, true, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "metric.reporters=" + CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER
                        + ",org.apache.kafka.common.metrics.JmxReporter"
                        + "," + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS,
                "kafka.metrics.reporters=" + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS,
                "min.insync.replicas=1"));
    }

    static Stream<Arguments> sourceUserConfigWithMetricsReporters() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("metric.reporters", "my.domain.CustomMetricReporter");
        configMap.put("kafka.metrics.reporters", "my.domain.CustomYammerMetricReporter");
        KafkaConfiguration userConfig = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, configMap.entrySet());

        String expectedConfig = "node.id=2\n"
                + "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir\n"
                + "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider\n"
                + "config.providers.strimzienv.param.allowlist.pattern=.*\n"
                + "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider\n"
                + "config.providers.strimzifile.param.allowed.paths=/opt/kafka\n"
                + "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider\n"
                + "config.providers.strimzidir.param.allowed.paths=/opt/kafka\n"
                + "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider\n"
                + "min.insync.replicas=1\n";

        // testing 8 combinations of 3 boolean values
        return Stream.of(
               Arguments.of(userConfig, false, false, false, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter"
               ),

               Arguments.of(userConfig, true, false, false, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter,com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter"),

               Arguments.of(userConfig, false, true, false, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter,org.apache.kafka.common.metrics.JmxReporter\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter"),

               Arguments.of(userConfig, false, false, true, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter," + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS + "\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter," + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS),

               Arguments.of(userConfig, true, true, false, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter,com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter,org.apache.kafka.common.metrics.JmxReporter\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter"),

               Arguments.of(userConfig, true, false, true, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter,com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter," + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS + "\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter," + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS),

               Arguments.of(userConfig, false, true, true, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter,org.apache.kafka.common.metrics.JmxReporter," +  StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS + "\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter," + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS),

               Arguments.of(userConfig, true, true, true, expectedConfig
                       + "metric.reporters=my.domain.CustomMetricReporter,com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter,org.apache.kafka.common.metrics.JmxReporter," + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS + "\n"
                       + "kafka.metrics.reporters=my.domain.CustomYammerMetricReporter," + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS)
        );
    }

    @ParameterizedTest
    @MethodSource("sourceUserConfigWithMetricsReporters")
    public void testUserConfigurationWithMetricReporters(
            KafkaConfiguration userConfig,
            boolean injectCruiseControl,
            boolean injectJmx,
            boolean injectStrimzi,
            String expectedConfig) {
        String actualConfig = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(userConfig, injectCruiseControl, injectJmx, injectStrimzi)
                .build();

        assertThat(actualConfig, isEquivalent(expectedConfig));
    }

    @Test
    public void testStrimziMetricsReporterViaUserAndMetricsConfigs() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("metric.reporters", StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS);
        configMap.put("kafka.metrics.reporters", StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS);
        KafkaConfiguration userConfig = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, configMap.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(userConfig, false, false, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "metric.reporters=" + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS,
                "kafka.metrics.reporters=" + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS,
                "min.insync.replicas=1"));
    }

    @Test
    public void testEphemeralStorageLogDirs()  {
        Storage storage = new EphemeralStorageBuilder()
                .withSizeLimit("5Gi")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, false))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "log.dirs=/var/lib/kafka/data/kafka-log2"));
    }

    @Test
    public void testPersistentStorageLogDirs()  {
        Storage storage = new PersistentClaimStorageBuilder()
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, false))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "log.dirs=/var/lib/kafka/data/kafka-log2"));
    }

    @Test
    public void testJbodStorageLogDirs()  {
        SingleVolumeStorage vol1 = new PersistentClaimStorageBuilder()
                .withId(1)
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        SingleVolumeStorage vol2 = new EphemeralStorageBuilder()
                .withId(2)
                .withSizeLimit("5Gi")
                .build();

        SingleVolumeStorage vol5 = new PersistentClaimStorageBuilder()
                .withId(5)
                .withSize("10Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(false)
                .build();

        Storage storage = new JbodStorageBuilder()
                .withVolumes(vol1, vol2, vol5)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, false))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "log.dirs=/var/lib/kafka/data-1/kafka-log2,/var/lib/kafka/data-2/kafka-log2,/var/lib/kafka/data-5/kafka-log2"));
    }

    @Test
    public void testWithNoListeners() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", emptyList(), null, null)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testConnectionLimits()  {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9100)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withMaxConnections(100)
                    .withMaxConnectionCreationRate(10)
                .endConfiguration()
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9101)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withMaxConnections(1000)
                    .withMaxConnectionCreationRate(50)
                .endConfiguration()
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener3")
                .withPort(9102)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                .endConfiguration()
                .build();

        GenericKafkaListener listener4 = new GenericKafkaListenerBuilder()
                .withName("listener4")
                .withPort(9103)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", asList(listener1, listener2, listener3, listener4), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.listener1-9100.max.connections=100",
                "listener.name.listener1-9100.max.connection.creation.rate=10",
                "listener.name.listener2-9101.max.connections=1000",
                "listener.name.listener2-9101.max.connection.creation.rate=50",
                "listeners=REPLICATION-9091://0.0.0.0:9091,LISTENER1-9100://0.0.0.0:9100,LISTENER2-9101://0.0.0.0:9101,LISTENER3-9102://0.0.0.0:9102,LISTENER4-9103://0.0.0.0:9103",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,LISTENER1-9100://dummy-advertised-address:1919,LISTENER2-9101://dummy-advertised-address:1919,LISTENER3-9102://dummy-advertised-address:1919,LISTENER4-9103://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,LISTENER1-9100:PLAINTEXT,LISTENER2-9101:PLAINTEXT,LISTENER3-9102:PLAINTEXT,LISTENER4-9103:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testWithPlainListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testKraftListenersMixedNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-kafka-0", 0, "kafka", true, true),
                new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true),
                new NodeRef("my-cluster-kafka-2", 2, "kafka", true, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        // KRaft controller or mixed node with version 3.9 or later should have advertised listeners configured with controller listener
        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testKraftListenersBrokerAndControllerNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
                new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
                new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
                new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
                new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
                new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        // Controller-only node
        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=controller",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-controllers-2:my-cluster-controllers-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-controllers-2:my-cluster-controllers-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));

        nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 11).findFirst().get();
        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=11",
                "process.roles=broker",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-brokers-11:my-cluster-brokers-11.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-brokers-11:my-cluster-brokers-11.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-brokers-11:my-cluster-brokers-11.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-brokers-11:my-cluster-brokers-11.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testWithPlainListenersWithSaslAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationScramSha512Auth()
                .endKafkaListenerAuthenticationScramSha512Auth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required ;",
                "listener.name.plain-9092.sasl.enabled.mechanisms=SCRAM-SHA-512"));
    }

    @Test
    public void testWithTlsListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.tls-9093.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.tls-9093.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithTlsListenersWithTlsAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.client.auth=required",
                "listener.name.tls-9093.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:clients-ca.crt}",
                "listener.name.tls-9093.ssl.truststore.type=PEM",
                "listener.name.tls-9093.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.tls-9093.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.tls-9093.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithTlsListenersWithCustomCerts()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("my-secret")
                        .withKey("my.key")
                        .withCertificate("my.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:tls-9093.crt}",
                "listener.name.tls-9093.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:tls-9093.key}",
                "listener.name.tls-9093.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalRouteListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalRouteListenersWithTlsAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.client.auth=required",
                "listener.name.external-9094.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:clients-ca.crt}",
                "listener.name.external-9094.ssl.truststore.type=PEM",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalRouteListenersWithSaslAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewKafkaListenerAuthenticationScramSha512Auth()
                .endKafkaListenerAuthenticationScramSha512Auth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SASL_SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required ;",
                "listener.name.external-9094.sasl.enabled.mechanisms=SCRAM-SHA-512",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalRouteListenersWithCustomCerts()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("my-secret")
                        .withKey("my.key")
                        .withCertificate("my.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:external-9094.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:external-9094.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalListenersLoadBalancerWithTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testPerBrokerWithExternalListeners()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "my-lb.com", listenerId -> "9094")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://my-lb.com:9094",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalListenersLoadBalancerWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testWithExternalListenersNodePortWithTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalListenersNodePortWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testPerBrokerWithExternalListenersNodePortWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "${strimzienv:STRIMZI_NODEPORT_DEFAULT_ADDRESS}", listenerId -> "31234")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${strimzienv:STRIMZI_NODEPORT_DEFAULT_ADDRESS}:31234",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testWithExternalListenersIngress()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withHost("broker-0.mytld.com")
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .withNewConfiguration()
                    .withControllerClass("nginx-ingress")
                    .withNewBootstrap()
                        .withHost("bootstrap.mytld.com")
                    .endBootstrap()
                    .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalListenersClusterIPWithTLS()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("ingress.mytld.com")
                .withAdvertisedPort(12345)
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(true)
                .withNewConfiguration()
                .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.external-9094.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.external-9094.ssl.keystore.type=PEM"));
    }

    @Test
    public void testWithExternalListenersClusterIPWithoutTLS()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("ingress.mytld.com")
                .withAdvertisedPort(12345)
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(false)
                .withNewConfiguration()
                .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testCustomAuthConfigSetProtocolMapCorrectlyForsSslSasl() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_SSL"));
    }

    @Test
    public void testCustomAuthConfigSetProtocolMapCorrectlyForPlainSasl() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_PLAINTEXT"));
    }


    @Test
    public void testCustomAuthConfigSetProtocolMapCorrectlyForPlain() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(false)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:PLAINTEXT"));
    }

    @Test
    public void testCustomAuthConfigRemovesForbiddenPrefixes() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(false)
                .withListenerConfig(Map.of("ssl.keystore.path", "foo"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, not(containsString("ssl.keystore.path")));
    }

    @Test
    public void testCustomAuthConfigPrefixesUserProvidedConfig() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of("oauthbearer.sasl.client.callback.handler.class", "client.class",
                        "oauthbearer.sasl.server.callback.handler.class", "server.class",
                        "oauthbearer.sasl.login.callback.handler.class", "login.class",
                        "oauthbearer.connections.max.reauth.ms", 999999999,
                        "sasl.enabled.mechanisms", "oauthbearer",
                        "oauthbearer.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.custom-listener-9092.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.custom-listener-9092.ssl.keystore.type=PEM",
                "listeners=REPLICATION-9091://0.0.0.0:9091,CUSTOM-LISTENER-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,CUSTOM-LISTENER-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.custom-listener-9092.sasl.enabled.mechanisms=oauthbearer",
                "listener.name.custom-listener-9092.oauthbearer.sasl.client.callback.handler.class=client.class",
                "listener.name.custom-listener-9092.oauthbearer.sasl.server.callback.handler.class=server.class",
                "listener.name.custom-listener-9092.oauthbearer.sasl.login.callback.handler.class=login.class",
                "listener.name.custom-listener-9092.oauthbearer.connections.max.reauth.ms=999999999",
                "listener.name.custom-listener-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"));
    }

    @Test
    public void testCustomTlsAuth() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                    .withSasl(false)
                    .withListenerConfig(Map.of("ssl.client.auth", "required",
                            "ssl.principal.mapping.rules", "RULE:^CN=(.*?),(.*)$/CN=$1/",
                            "ssl.truststore.location", "/opt/kafka/custom-authn-secrets/custom-listener-external-9094/custom-truststore/ca.crt",
                            "ssl.truststore.type", "PEM"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.controlplane-9090.ssl.keystore.type=PEM",
                "listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.controlplane-9090.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.replication-9091.ssl.keystore.type=PEM",
                "listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-trustbundle:cluster-ca.crt}",
                "listener.name.replication-9091.ssl.truststore.type=PEM",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "listener.name.custom-listener-9092.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "listener.name.custom-listener-9092.ssl.keystore.type=PEM",
                "listener.name.custom-listener-9092.ssl.truststore.location=/opt/kafka/custom-authn-secrets/custom-listener-external-9094/custom-truststore/ca.crt",
                "listener.name.custom-listener-9092.ssl.truststore.type=PEM",
                "listener.name.custom-listener-9092.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.principal.mapping.rules=RULE:^CN=(.*?),(.*)$/CN=$1/",
                "listeners=REPLICATION-9091://0.0.0.0:9091,CUSTOM-LISTENER-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,CUSTOM-LISTENER-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @Test
    public void testWithTieredStorage() {
        TieredStorageCustom tieredStorage = new TieredStorageCustom();
        RemoteStorageManager rsm = new RemoteStorageManager();
        rsm.setClassName("com.example.kafka.tiered.storage.s3.S3RemoteStorageManager");
        rsm.setClassPath("/opt/kafka/plugins/tiered-storage-s3/*");
        Map<String, String> rsmConfigs = new HashMap<>();
        rsmConfigs.put("storage.bucket.name", "my-bucket");
        rsm.setConfig(rsmConfigs);
        tieredStorage.setRemoteStorageManager(rsm);
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withTieredStorage("test-cluster-1", tieredStorage)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "remote.log.storage.system.enable=true",
                "remote.log.metadata.manager.impl.prefix=rlmm.config.",
                "remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager",
                "remote.log.metadata.manager.listener.name=REPLICATION-9091",
                "rlmm.config.remote.log.metadata.common.client.bootstrap.servers=test-cluster-1-kafka-brokers:9091",
                "rlmm.config.remote.log.metadata.common.client.security.protocol=SSL",
                "rlmm.config.remote.log.metadata.common.client.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
                "rlmm.config.remote.log.metadata.common.client.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
                "rlmm.config.remote.log.metadata.common.client.ssl.keystore.type=PEM",
                "rlmm.config.remote.log.metadata.common.client.ssl.truststore.certificates=${strimzisecrets:namespace/test-cluster-1-trustbundle:cluster-ca.crt}",
                "rlmm.config.remote.log.metadata.common.client.ssl.truststore.type=PEM",
                "remote.log.storage.manager.class.name=com.example.kafka.tiered.storage.s3.S3RemoteStorageManager",
                "remote.log.storage.manager.class.path=/opt/kafka/plugins/tiered-storage-s3/*",
                "remote.log.storage.manager.impl.prefix=rsm.config.",
                "rsm.config.storage.bucket.name=my-bucket"
        ));
    }

    @Test
    public void testSimpleAuthorizationOnMigration() {
        NodeRef broker = new NodeRef("my-cluster-brokers-0", 0, "brokers", false, true);
        NodeRef controller = new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false);

        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, controller)
                .withAuthorization("my-cluster", auth)
                .build();
        assertThat(configuration, isEquivalent("node.id=1",
                "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));

        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, broker)
                .withAuthorization("my-cluster", auth)
                .build();
        assertThat(configuration, containsString("authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"));
    }

    @Test
    public void testWithStrimziQuotas() {
        QuotasPluginStrimzi quotasPluginStrimzi = new QuotasPluginStrimziBuilder()
            .withConsumerByteRate(1000L)
            .withProducerByteRate(1000L)
            .withExcludedPrincipals("User:my-user1", "User:my-user2")
            .withMinAvailableBytesPerVolume(200000L)
            .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withQuotas("my-personal-cluster", quotasPluginStrimzi)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
            "client.quota.callback.class=io.strimzi.kafka.quotas.StaticQuotaCallback",
            "client.quota.callback.static.kafka.admin.bootstrap.servers=my-personal-cluster-kafka-brokers:9091",
            "client.quota.callback.static.kafka.admin.security.protocol=SSL",
            "client.quota.callback.static.kafka.admin.ssl.keystore.certificate.chain=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.crt}",
            "client.quota.callback.static.kafka.admin.ssl.keystore.key=${strimzisecrets:namespace/my-cluster-kafka-2:my-cluster-kafka-2.key}",
            "client.quota.callback.static.kafka.admin.ssl.keystore.type=PEM",
            "client.quota.callback.static.kafka.admin.ssl.truststore.certificates=${strimzisecrets:namespace/my-personal-cluster-trustbundle:cluster-ca.crt}",
            "client.quota.callback.static.kafka.admin.ssl.truststore.type=PEM",
            "client.quota.callback.static.produce=1000",
            "client.quota.callback.static.fetch=1000",
            "client.quota.callback.static.storage.per.volume.limit.min.available.bytes=200000",
            "client.quota.callback.static.excluded.principal.name.list=User:CN=my-personal-cluster-kafka,O=io.strimzi;User:CN=my-personal-cluster-cruise-control,O=io.strimzi;User:my-user1;User:my-user2"
        ));
    }

    @Test
    public void testWithNullQuotas() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withQuotas("my-personal-cluster", null)
            .build();

        assertThat(configuration, not(containsString("client.quota.callback.class")));
        assertThat(configuration, not(containsString("client.quota.callback.static")));
    }

    @Test
    public void testWithKafkaQuotas() {
        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withConsumerByteRate(1000L)
            .withProducerByteRate(1000L)
            .withRequestPercentage(33)
            .withControllerMutationRate(0.5)
            .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withQuotas("my-personal-cluster", quotasPluginKafka)
            .build();

        assertThat(configuration, not(containsString("client.quota.callback.class")));
        assertThat(configuration, not(containsString("client.quota.callback.static")));
    }

    @Test
    public void testWithKRaftMetadataLogDir() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withKRaftMetadataLogDir("/my/kraft/metadata")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "metadata.log.dir=/my/kraft/metadata/kafka-log2"
        ));
    }

    @Test
    public void testDefaultMinInSyncReplicasWhenNotSpecified() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("auto.create.topics.enable", "false");
        userConfiguration.put("offsets.topic.replication.factor", 3);

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzisecrets,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "min.insync.replicas=1",
                "auto.create.topics.enable=false",
                "offsets.topic.replication.factor=3"));
    }
}

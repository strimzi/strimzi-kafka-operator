/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.TestUtils;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneEncryptionConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.model.jmx.JmxModel;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.ca.InternalCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.CLUSTER;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.NAMESPACE;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createBrokerPool;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createControllerPool;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createKafka;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createMixedPool;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class KafkaClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static Kafka KAFKA = createKafka();
    private final static KafkaNodePool POOL_CONTROLLERS = createControllerPool();
    private final static KafkaNodePool POOL_MIXED = createMixedPool();
    private final static KafkaNodePool POOL_BROKERS = createBrokerPool();
    private final static Map<Integer, Map<String, String>> ADVERTISED_HOSTNAMES = Map.of(
            3, Map.of("PLAIN_9092", "mixed-3", "TLS_9093", "mixed-3"),
            4, Map.of("PLAIN_9092", "mixed-4", "TLS_9093", "mixed-4"),
            5, Map.of("PLAIN_9092", "broker-5", "TLS_9093", "broker-5"),
            6, Map.of("PLAIN_9092", "broker-6", "TLS_9093", "broker-6"),
            7, Map.of("PLAIN_9092", "broker-7", "TLS_9093", "broker-7")
    );
    private final static Map<Integer, Map<String, String>> ADVERTISED_PORTS = Map.of(
            3, Map.of("PLAIN_9092", "9092", "TLS_9093", "10003"),
            4, Map.of("PLAIN_9092", "9092", "TLS_9093", "10004"),
            5, Map.of("PLAIN_9092", "9092", "TLS_9093", "10005"),
            6, Map.of("PLAIN_9092", "9092", "TLS_9093", "10006"),
            7, Map.of("PLAIN_9092", "9092", "TLS_9093", "10007")
    );

    private static final List<KafkaPool> POOLS = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

    //////////
    // Utility methods
    //////////
    private Map<String, String> expectedSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
    }

    private Map<String, String> expectedBrokerSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_BROKER_ROLE_LABEL, "true");
    }

    private void checkHeadlessService(Service headless) {
        assertThat(headless.getMetadata().getName(), is(KafkaResources.brokersServiceName(CLUSTER)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(5));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.KAFKA_AGENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(KafkaCluster.KAFKA_AGENT_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(9092));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(4).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(4).getPort(), is(9093));
        assertThat(headless.getSpec().getPorts().get(4).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headless.getSpec().getIpFamilies(), is(nullValue()));

        assertThat(headless.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(false));
    }

    private List<Secret> generateBrokerSecrets(Set<String> externalBootstrapAddress, Map<Integer, Set<String>> externalAddresses) {
        InternalCa clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(), new PasswordGenerator(10, "a", "a"), null, null, CaConfig.createDefault());
        clusterCa.createRenewOrReplace(true, false, false);
        return KC.generateCertificatesSecrets(clusterCa, List.of(), Map.of(), externalBootstrapAddress, externalAddresses, true).toCompletableFuture().join();
    }

    //////////
    // Tests
    //////////

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName("kafka-metrics-config")
                .endMetadata()
                .withData(singletonMap("kafka-metrics-config.yml", "{\"animal\":\"wombat\"}"))
                .build();
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("kafka-metrics-config.yml", "kafka-metrics-config", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(metricsCm, null), ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, Set.of());
        assertThat(cms.size(), is(8));

        for (ConfigMap cm : cms)    {
            if (cm.getMetadata().getName().contains("controllers")) {
                TestUtils.checkOwnerReference(cm, POOL_CONTROLLERS);
            } else if (cm.getMetadata().getName().contains("mixed")) {
                TestUtils.checkOwnerReference(cm, POOL_MIXED);
            } else {
                TestUtils.checkOwnerReference(cm, POOL_BROKERS);
            }

            assertThat(cm.getData().get(JmxPrometheusExporterModel.CONFIG_MAP_KEY), is("{\"animal\":\"wombat\"}"));
        }
    }

    @Test
    public void testStrimziMetricsReporterConfig() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_log.*", "kafka_network.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(null, null), ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, Set.of());
        assertThat(cms.size(), is(8));

        for (ConfigMap cm : cms) {
            assertThat(cm.getData().toString(), containsString("metric.reporters=" + StrimziMetricsReporterConfig.SERVER_KAFKA_CLASS));
            assertThat(cm.getData().toString(), containsString("kafka.metrics.reporters=" + StrimziMetricsReporterConfig.SERVER_YAMMER_CLASS));
            assertThat(cm.getData().toString(), containsString(StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true"));
            assertThat(cm.getData().toString(), containsString(StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT));
            assertThat(cm.getData().toString(), containsString(StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_log.*,kafka_network.*"));
        }

        NetworkPolicy np = kc.generateNetworkPolicy(null, null);
        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(StrimziMetricsReporterModel.METRICS_PORT))).toList();

        assertThat(rules.size(), is(1));
    }

    @Test
    public void testGenerateService() {
        Service clusterIp = KC.generateService();

        assertThat(clusterIp.getSpec().getType(), is("ClusterIP"));
        assertThat(clusterIp.getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(clusterIp.getSpec().getPorts().size(), is(3));
        assertThat(clusterIp.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(clusterIp.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(1).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(1).getPort(), is(9092));
        assertThat(clusterIp.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(2).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(2).getPort(), is(9093));
        assertThat(clusterIp.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(clusterIp.getSpec().getIpFamilies(), is(nullValue()));
        assertThat(clusterIp.getSpec().getPublishNotReadyAddresses(), is(nullValue()));
        
        assertThat(clusterIp.getMetadata().getAnnotations(), hasKey("strimzi.io/discovery"));        
        JsonArray annotation = new JsonArray(clusterIp.getMetadata().getAnnotations().get("strimzi.io/discovery"));
        JsonObject listener1 = annotation.getJsonObject(0);
        assertThat(listener1.getString("port"), is("9092"));
        assertThat(listener1.getString("tls"), is("false"));
        assertThat(listener1.getString("protocol"), is("kafka"));
        assertThat(listener1.getString("auth"), is("none"));
        JsonObject listener2 = annotation.getJsonObject(1);
        assertThat(listener2.getString("port"), is("9093"));
        assertThat(listener2.getString("tls"), is("true"));
        assertThat(listener2.getString("protocol"), is("kafka"));
        assertThat(listener2.getString("auth"), is("none"));

        assertThat(clusterIp.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(true));
        assertThat(clusterIp.getMetadata().getLabels().get(Labels.STRIMZI_DISCOVERY_LABEL), is("true"));

        TestUtils.checkOwnerReference(clusterIp, KAFKA);
    }

    @Test
    public void testGenerateServiceWithoutMetrics() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetricsConfig(null)
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        Service clusterIp = kc.generateService();

        assertThat(clusterIp.getSpec().getType(), is("ClusterIP"));
        assertThat(clusterIp.getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(clusterIp.getSpec().getPorts().size(), is(3));
        assertThat(clusterIp.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(clusterIp.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(1).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(1).getPort(), is(9092));
        assertThat(clusterIp.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(2).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(2).getPort(), is(9093));
        assertThat(clusterIp.getSpec().getPorts().get(2).getProtocol(), is("TCP"));

        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        assertThat(clusterIp.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(true));
        assertThat(clusterIp.getMetadata().getLabels().get(Labels.STRIMZI_DISCOVERY_LABEL), is("true"));

        TestUtils.checkOwnerReference(clusterIp, KAFKA);
    }

    @Test
    public void testGenerateHeadlessServiceWithJmxMetrics() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        Service headless = kc.generateHeadlessService();

        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(6));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.KAFKA_AGENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(KafkaCluster.KAFKA_AGENT_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(9092));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(4).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(4).getPort(), is(9093));
        assertThat(headless.getSpec().getPorts().get(4).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(5).getName(), is(JmxModel.JMX_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(5).getPort(), is(JmxModel.JMX_PORT));
        assertThat(headless.getSpec().getPorts().get(5).getProtocol(), is("TCP"));

        assertThat(headless.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(false));

        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @Test
    public void testAuxiliaryResourcesTemplate() {
        Map<String, String> pdbLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> pdbAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> crbLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> crbAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> saAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> jmxLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> jmxAnnotations = Map.of("a7", "v7", "a8", "v8");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("external2")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build())
                        .withNewJmxOptions()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                        .endJmxOptions()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnnotations)
                                .endMetadata()
                            .endPodDisruptionBudget()
                            .withNewClusterRoleBinding()
                                .withNewMetadata()
                                    .withLabels(crbLabels)
                                    .withAnnotations(crbAnnotations)
                                .endMetadata()
                            .endClusterRoleBinding()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnnotations)
                                .endMetadata()
                            .endServiceAccount()
                            .withNewJmxSecret()
                                .withNewMetadata()
                                    .withLabels(jmxLabels)
                                    .withAnnotations(jmxAnnotations)
                                .endMetadata()
                            .endJmxSecret()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kc.generateClusterRoleBinding("namespace");
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnnotations.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));

        // Check JMX Secret
        Secret jmxSecret = kc.jmx().jmxSecret(null);
        assertThat(jmxSecret.getMetadata().getLabels().entrySet().containsAll(jmxLabels.entrySet()), is(true));
        assertThat(jmxSecret.getMetadata().getAnnotations().entrySet().containsAll(jmxAnnotations.entrySet()), is(true));
    }

    @Test
    public void testGenerateClusterOperatorServiceAccount() {
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER,
                new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), AuthenticationConfiguration.fromCrd(NAMESPACE, CLUSTER, new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build())));

        ServiceAccount sa = kc.generateClusterOperatorServiceAccount();
        assertThat(sa, is(notNullValue()));
        assertThat(sa.getMetadata().getName(), is(KafkaResources.clusterOperatorServiceAccount(CLUSTER)));
        assertThat(sa.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sa.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(CLUSTER));
    }

    @Test
    public void testGenerateClusterOperatorServiceAccountWithoutServiceAccountAuthentication() {
        // Without Service Account authentication, the Service Account should not exist
        assertThat(KC.generateClusterOperatorServiceAccount(), is(nullValue()));

        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER,
                new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration()));
        assertThat(kc.generateClusterOperatorServiceAccount(), is(nullValue()));
    }

    @Test
    public void testJmxSecret() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                  .build())
                            .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        Secret jmxSecret = kc.jmx().jmxSecret(null);

        assertThat(jmxSecret.getData(), hasKey("jmx-username"));
        assertThat(jmxSecret.getData(), hasKey("jmx-password"));

        Secret newJmxSecret = kc.jmx().jmxSecret(jmxSecret);

        assertThat(newJmxSecret.getData(), hasKey("jmx-username"));
        assertThat(newJmxSecret.getData(), hasKey("jmx-password"));
        assertThat(newJmxSecret.getData().get("jmx-username"), is(jmxSecret.getData().get("jmx-username")));
        assertThat(newJmxSecret.getData().get("jmx-password"), is(jmxSecret.getData().get("jmx-password")));
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = KC.generateHeadlessService();
        checkHeadlessService(headless);
        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @Test
    public void testPerBrokerConfiguration() {
        String config = KC.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(config, CoreMatchers.containsString("node.id=1"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log1"));
        assertThat(config, CoreMatchers.containsString("\nlisteners=CONTROLPLANE-9090://0.0.0.0:9090\n"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=CONTROLPLANE-9090://foo-controllers-1.foo-kafka-brokers.test.svc:9090\n"));
        assertThat(config, CoreMatchers.containsString("process.roles=controller\n"));
        assertThat(config, CoreMatchers.containsString("controller.quorum.voters=0@foo-controllers-0.foo-kafka-brokers.test.svc:9090,1@foo-controllers-1.foo-kafka-brokers.test.svc:9090,2@foo-controllers-2.foo-kafka-brokers.test.svc:9090,3@foo-mixed-3.foo-kafka-brokers.test.svc:9090,4@foo-mixed-4.foo-kafka-brokers.test.svc:9090\n"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/foo-controllers-1:foo-controllers-1.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/foo-controllers-1:foo-controllers-1.key}"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.keystore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/foo-trustbundle:cluster-ca.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.truststore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.client.auth=required"));

        config = KC.generatePerBrokerConfiguration(4, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(config, CoreMatchers.containsString("node.id=4"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log4"));
        assertThat(config, CoreMatchers.containsString("\nlisteners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092,TLS-9093://0.0.0.0:9093\n"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=CONTROLPLANE-9090://foo-mixed-4.foo-kafka-brokers.test.svc:9090,REPLICATION-9091://foo-mixed-4.foo-kafka-brokers.test.svc:9091,PLAIN-9092://mixed-4:9092,TLS-9093://mixed-4:10004\n"));
        assertThat(config, CoreMatchers.containsString("process.roles=broker,controller\n"));
        assertThat(config, CoreMatchers.containsString("controller.quorum.voters=0@foo-controllers-0.foo-kafka-brokers.test.svc:9090,1@foo-controllers-1.foo-kafka-brokers.test.svc:9090,2@foo-controllers-2.foo-kafka-brokers.test.svc:9090,3@foo-mixed-3.foo-kafka-brokers.test.svc:9090,4@foo-mixed-4.foo-kafka-brokers.test.svc:9090\n"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.keystore.certificate.chain=${strimzisecrets:namespace/foo-mixed-4:foo-mixed-4.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.keystore.key=${strimzisecrets:namespace/foo-mixed-4:foo-mixed-4.key}"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.keystore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.truststore.certificates=${strimzisecrets:namespace/foo-trustbundle:cluster-ca.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.truststore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.controlplane-9090.ssl.client.auth=required"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/foo-mixed-4:foo-mixed-4.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/foo-mixed-4:foo-mixed-4.key}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/foo-trustbundle:cluster-ca.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.truststore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.client.auth=required"));

        config = KC.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(config, CoreMatchers.containsString("node.id=6"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log6"));
        assertThat(config, CoreMatchers.containsString("\nlisteners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092,TLS-9093://0.0.0.0:9093\n"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=REPLICATION-9091://foo-brokers-6.foo-kafka-brokers.test.svc:9091,PLAIN-9092://broker-6:9092,TLS-9093://broker-6:10006\n"));
        assertThat(config, CoreMatchers.containsString("process.roles=broker\n"));
        assertThat(config, CoreMatchers.containsString("controller.quorum.voters=0@foo-controllers-0.foo-kafka-brokers.test.svc:9090,1@foo-controllers-1.foo-kafka-brokers.test.svc:9090,2@foo-controllers-2.foo-kafka-brokers.test.svc:9090,3@foo-mixed-3.foo-kafka-brokers.test.svc:9090,4@foo-mixed-4.foo-kafka-brokers.test.svc:9090\n"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.certificate.chain=${strimzisecrets:namespace/foo-brokers-6:foo-brokers-6.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/foo-brokers-6:foo-brokers-6.key}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.key=${strimzisecrets:namespace/foo-brokers-6:foo-brokers-6.key}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.keystore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.truststore.certificates=${strimzisecrets:namespace/foo-trustbundle:cluster-ca.crt}"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.truststore.type=PEM"));
        assertThat(config, CoreMatchers.containsString("listener.name.replication-9091.ssl.client.auth=required"));
    }

    @Test
    public void testPerBrokerConfigurationCordoning() {
        // Cordoned broker: cordoned.log.dirs=* should be present
        String config = KC.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, true);
        assertThat(config, CoreMatchers.containsString("cordoned.log.dirs=*"));

        // Not cordoned broker: cordoned.log.dirs should not be present
        config = KC.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(config, not(CoreMatchers.containsString("cordoned.log.dirs")));
    }

    @Test
    public void testPerBrokerConfigMapsCordoning() {
        MetricsAndLogging metricsAndLogging = new MetricsAndLogging(null, null);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, "dummy-cluster-id", SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, Set.of(6));

        for (ConfigMap cm : cms) {
            String brokerConfig = cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME);
            if (cm.getMetadata().getName().contains("brokers-6")) {
                assertThat(brokerConfig, CoreMatchers.containsString("cordoned.log.dirs=*"));
            } else {
                assertThat(brokerConfig, not(CoreMatchers.containsString("cordoned.log.dirs")));
            }
        }
    }

    @Test
    public void testPerBrokerConfigMaps() {
        MetricsAndLogging metricsAndLogging = new MetricsAndLogging(null, null);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, "dummy-cluster-id", SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, Set.of());

        assertThat(cms.size(), is(8));

        for (ConfigMap cm : cms)    {
            assertThat(cm.getMetadata().getName(), startsWith("foo-"));

            if (cm.getMetadata().getName().contains("controllers")) {
                assertThat(cm.getData().size(), is(5));
                assertThat(cm.getData().get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), CoreMatchers.containsString("process.roles=controller\n"));
                assertThat(cm.getData().get(KafkaCluster.AGENT_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("dummy-cluster-id"));
            } else if (cm.getMetadata().getName().contains("brokers")) {
                assertThat(cm.getData().size(), is(5));
                assertThat(cm.getData().get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), CoreMatchers.containsString("process.roles=broker\n"));
                assertThat(cm.getData().get(KafkaCluster.AGENT_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("dummy-cluster-id"));
            } else {
                assertThat(cm.getData().size(), is(5));
                assertThat(cm.getData().get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), CoreMatchers.containsString("process.roles=broker,controller\n"));
                assertThat(cm.getData().get(KafkaCluster.AGENT_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("dummy-cluster-id"));
            }
        }
    }

    @Test
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        List<Secret> secrets = generateBrokerSecrets(null, Map.of());
        Set<String> secretDataKeys = new HashSet<>();
        Map<String, Secret> secretMap = new HashMap<>();
        secrets.forEach(secret -> {
            secretDataKeys.addAll(secret.getData().keySet());
            secretMap.put(secret.getMetadata().getName(), secret);
        });
        assertThat(secretDataKeys, is(Set.of(
                "foo-controllers-0.crt",  "foo-controllers-0.key",
                "foo-controllers-1.crt", "foo-controllers-1.key",
                "foo-controllers-2.crt", "foo-controllers-2.key",
                "foo-mixed-3.crt",  "foo-mixed-3.key",
                "foo-mixed-4.crt", "foo-mixed-4.key",
                "foo-brokers-5.crt",  "foo-brokers-5.key",
                "foo-brokers-6.crt", "foo-brokers-6.key",
                "foo-brokers-7.crt", "foo-brokers-7.key")));

        X509Certificate cert = CertificateUtils.cert(secretMap.get("foo-controllers-0"), "foo-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = CertificateUtils.cert(secretMap.get("foo-mixed-3"), "foo-mixed-3.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = CertificateUtils.cert(secretMap.get("foo-brokers-6"), "foo-brokers-6.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));
    }

    @Test
    public void testGenerateBrokerSecretExternal() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = Map.of(
                0, Set.of("123.10.125.130"),
                3, Set.of("123.10.125.133"),
                6, Set.of("123.10.125.136"));

        List<Secret> secrets = generateBrokerSecrets(Set.of("123.10.125.140"), externalAddresses);
        Set<String> secretDataKeys = new HashSet<>();
        Map<String, Secret> secretMap = new HashMap<>();
        secrets.forEach(secret -> {
            secretDataKeys.addAll(secret.getData().keySet());
            secretMap.put(secret.getMetadata().getName(), secret);
        });
        assertThat(secretDataKeys, is(Set.of(
                "foo-controllers-0.crt",  "foo-controllers-0.key",
                "foo-controllers-1.crt", "foo-controllers-1.key",
                "foo-controllers-2.crt", "foo-controllers-2.key",
                "foo-mixed-3.crt",  "foo-mixed-3.key",
                "foo-mixed-4.crt", "foo-mixed-4.key",
                "foo-brokers-5.crt",  "foo-brokers-5.key",
                "foo-brokers-6.crt", "foo-brokers-6.key",
                "foo-brokers-7.crt", "foo-brokers-7.key")));

        X509Certificate cert = CertificateUtils.cert(secretMap.get("foo-controllers-0"), "foo-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = CertificateUtils.cert(secretMap.get("foo-mixed-3"), "foo-mixed-3.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(12));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.133"))));

        cert = CertificateUtils.cert(secretMap.get("foo-brokers-6"), "foo-brokers-6.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(12));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.136"))));
    }

    @Test
    public void testGenerateBrokerSecretExternalWithManyDNS() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = Map.of(
                0, Set.of("123.10.125.130", "my-broker-0"),
                3, Set.of("123.10.125.133", "my-broker-3"),
                6, Set.of("123.10.125.136", "my-broker-6"));

        List<Secret> secrets = generateBrokerSecrets(Set.of("123.10.125.140", "my-bootstrap"), externalAddresses);
        Set<String> secretDataKeys = new HashSet<>();
        Map<String, Secret> secretMap = new HashMap<>();
        secrets.forEach(secret -> {
            secretDataKeys.addAll(secret.getData().keySet());
            secretMap.put(secret.getMetadata().getName(), secret);
        });

        assertThat(secretDataKeys, is(Set.of(
                "foo-controllers-0.crt",  "foo-controllers-0.key",
                "foo-controllers-1.crt", "foo-controllers-1.key",
                "foo-controllers-2.crt", "foo-controllers-2.key",
                "foo-mixed-3.crt",  "foo-mixed-3.key",
                "foo-mixed-4.crt", "foo-mixed-4.key",
                "foo-brokers-5.crt",  "foo-brokers-5.key",
                "foo-brokers-6.crt", "foo-brokers-6.key",
                "foo-brokers-7.crt", "foo-brokers-7.key")));

        X509Certificate cert = CertificateUtils.cert(secretMap.get("foo-controllers-0"), "foo-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = CertificateUtils.cert(secretMap.get("foo-mixed-3"), "foo-mixed-3.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(14));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "my-broker-3"),
                List.of(2, "my-bootstrap"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.133"))));

        cert = CertificateUtils.cert(secretMap.get("foo-brokers-6"), "foo-brokers-6.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(14));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "my-broker-6"),
                List.of(2, "my-bootstrap"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.136"))));
    }

    @Test
    public void testGenerateBrokerSecretWithCustomCerts() {
        InternalCa clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(), new PasswordGenerator(10, "a", "a"), null, null, CaConfig.createDefault());
        clusterCa.createRenewOrReplace(true, false, false);

        List<Secret> secrets = KC.generateCertificatesSecrets(clusterCa, List.of(), Map.of("listener1-9999.crt", "cert", "listener1-9999.key", "key"), null, Map.of(), true).toCompletableFuture().join();
        secrets.forEach(secret -> {
            assertThat(secret.getData().get("listener1-9999.crt"), is("cert"));
            assertThat(secret.getData().get("listener1-9999.key"), is("key"));
        });
    }

    @Test
    public void testControlPlanePortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER)))
                .endPodSelector()
                .build();
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        // Check Network Policies => Different namespace
        NetworkPolicy np = KC.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(2));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @Test
    public void testReplicationPortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer eoPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer kafkaExporterPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaExporterResources.componentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer cruiseControlPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_NAME_LABEL, CruiseControlResources.componentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeerSameNamespace = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeerNamespaceWithLabels = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector()
                    .withMatchLabels(Map.of("nsLabelKey", "nsLabelValue"))
                .endNamespaceSelector()
                .build();

        // Check Network Policies => Different namespace
        NetworkPolicy np = KC.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeer), is(true));

        // Check Network Policies => Same namespace
        np = KC.generateNetworkPolicy(NAMESPACE, null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeerSameNamespace), is(true));

        // Check Network Policies => Namespace with Labels
        np = KC.generateNetworkPolicy("operator-namespace", Labels.fromMap(Map.of("nsLabelKey", "nsLabelValue")));

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeerNamespaceWithLabels), is(true));
    }

    @Test
    public void testNetworkPolicyPeers() {
        NetworkPolicyPeer peer1 = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key1").withValues("my-value1").build())
                .endPodSelector()
                .build();

        NetworkPolicyPeer peer2 = new NetworkPolicyPeerBuilder()
                .withNewNamespaceSelector()
                    .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key2").withValues("my-value2").build())
                .endNamespaceSelector()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withNetworkPolicyPeers(peer1)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNetworkPolicyPeers(peer2)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .withNetworkPolicyPeers(peer1, peer2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Check Network Policies
        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9092))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer1));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9093))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer2));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9094))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(2));
        assertThat(rules.get(0).getFrom().contains(peer1), is(true));
        assertThat(rules.get(0).getFrom().contains(peer2), is(true));
    }

    @Test
    public void testNoNetworkPolicyPeers() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Check Network Policies
        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9092))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9093))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9094))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));
    }

    @Test
    public void testDefaultPodDisruptionBudget()   {
        PodDisruptionBudget pdb = KC.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(7));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().toMap()));
    }

    @Test
    public void testCustomizedPodDisruptionBudget()   {
        Map<String, String> pdbLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> pdbAnnotations = Map.of("a1", "v1", "a2", "v2");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withAnnotations(pdbAnnotations)
                                    .withLabels(pdbLabels)
                                .endMetadata()
                                .withMaxUnavailable(2)
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();

        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(6));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
    }

    @Test
    public void testClusterRoleBindingNodePort() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaResources.initContainerClusterRoleBindingName(CLUSTER, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @Test
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withNewTopologyLabelRack()
                            .withTopologyKey("my-topology-label")
                        .endTopologyLabelRack()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaResources.initContainerClusterRoleBindingName(CLUSTER, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @Test
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        ClusterRoleBinding crb = KC.generateClusterRoleBinding(testNamespace);

        assertThat(crb, is(nullValue()));
    }

    @Test
    public void testRoleAndRoleBinding() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                .withNamespace("namespace")
                .endMetadata()
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                        .build())
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        Role r = kc.generateRole();
        assertThat(r.getMetadata().getName(), is(kc.componentName));
        assertThat(r.getRules().get(0).getResources(), is(List.of("secrets")));
        assertThat(r.getRules().get(0).getResourceNames(), is(List.of("foo-cluster-ca-cert",
                "foo-clients-ca-cert",
                "foo-trustbundle",
                "foo-brokers-7",
                "foo-brokers-6",
                "foo-controllers-0",
                "foo-controllers-1",
                "foo-mixed-3",
                "foo-controllers-2",
                "foo-mixed-4",
                "foo-brokers-5")));
        assertThat(r.getRules().get(0).getVerbs(), is(List.of("get")));

        RoleBinding rb = kc.generateRoleBindingForRole();
        assertThat(rb.getMetadata().getName(), is(KafkaResources.kafkaRoleBindingName(CLUSTER)));
        assertThat(rb.getSubjects().get(0).getName(), is(kc.componentName));
        assertThat(rb.getRoleRef().getName(), is(kc.componentName));
    }

    @Test
    public void testReplicasAndRelatedOptionsValidationNok() {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .editKafka()
                            .withConfig(Map.of("offsets.topic.replication.factor", 6))
                        .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });
        assertThat(ex.getMessage(), is("Kafka configuration option 'offsets.topic.replication.factor' should be set to " + 5 + " or less because this cluster has only " + 5 + " Kafka broker(s)."));
    }

    @Test
    public void testReplicasAndRelatedOptionsValidationOk() {
        assertDoesNotThrow(() -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .editKafka()
                            .withConfig(Map.of("offsets.topic.replication.factor", 5))
                        .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });
    }

    @Test
    public void testPerListenerConfigurations() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withConfig(Map.of("listener.name.tls-9093.max.connections", 1000,
                        "listener.name.tls-9093.connections.max.reauth.ms", 1000,
                        "listener.name.tls-9093.connections.max.idle.ms", 100000,
                        "listener.name.plain-9092.max.connections", 1000,
                        "listener.name.plain-9092.connections.max.reauth.ms", 1000,
                        "listener.name.plain-9092.connections.max.idle.ms", 100000,
                        "listener.name.CONTROLPLANE-9090.max.connections", 1000,
                        "listener.name.REPLICATION-9091.connections.max.reauth.ms", 1000,
                        "listener.name.does-not-exist.connections.max.reauth.ms", 1000))
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster cluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // configurations that are in the exception list
        assertThat(cluster.configuration.getConfiguration(), CoreMatchers.containsString("listener.name.tls-9093.connections.max.reauth.ms=1000"));
        assertThat(cluster.configuration.getConfiguration(), CoreMatchers.containsString("listener.name.tls-9093.max.connections=1000"));
        assertThat(cluster.configuration.getConfiguration(), CoreMatchers.containsString("listener.name.plain-9092.connections.max.reauth.ms=1000"));
        assertThat(cluster.configuration.getConfiguration(), CoreMatchers.containsString("listener.name.plain-9092.max.connections=1000"));

        // configuration that is not in the exception
        assertThat(cluster.configuration.getConfiguration(), not(CoreMatchers.containsString("listener.name.plain-9092.connections.max.idle.ms")));
        assertThat(cluster.configuration.getConfiguration(), not(CoreMatchers.containsString("listener.name.tls-9093.connections.max.idle.ms")));

        // configuration for a listener that does not exist
        assertThat(cluster.configuration.getConfiguration(), not(CoreMatchers.containsString("listener.name.does-not-exist.connections.max.reauth.ms")));

        // configurations for internal interfaces
        assertThat(cluster.configuration.getConfiguration(), not(CoreMatchers.containsString("listener.name.REPLICATION-9091.connections.max.reauth.ms")));
        assertThat(cluster.configuration.getConfiguration(), not(CoreMatchers.containsString("listener.name.CONTROLPLANE-9090.max.connections")));
    }

    @Test
    public void testCruiseControl() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))).build())
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("1500m"), "memory", new Quantity("18Gi"))).build())
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("2500m"), "memory", new Quantity("28Gi"))).build())
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(controllers, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        // Not set for controller only nodes
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.toString())));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.toString())));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.toString())));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + 1));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + 1));

        // Test values generated for Cruise Control capacity configuration
        Map<String, Storage> storage = kafkaCluster.getStorageByPoolName();
        assertThat(storage.size(), is(3));
        assertThat(storage.get("controllers"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(storage.get("mixed"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(storage.get("brokers"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));

        Map<String, ResourceRequirements> resources = kafkaCluster.getBrokerResourceRequirementsByPoolName();
        assertThat(resources.size(), is(2));
        assertThat(resources.get("mixed").getLimits(), is(Map.of("cpu", new Quantity("1500m"), "memory", new Quantity("18Gi"))));
        assertThat(resources.get("brokers").getLimits(), is(Map.of("cpu", new Quantity("2500m"), "memory", new Quantity("28Gi"))));
    }

    @Test
    public void testCruiseControlCustomMetricsReporterTopic() {
        int replicationFactor = 3;
        int minInSync = 2;
        int partitions = 5;

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.getValue(), partitions,
                                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue(), replicationFactor,
                                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSync
                        ))
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        // The metrics reporter is not configured in a controller only node
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions)));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor)));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync)));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));
    }

    @Test
    public void testCruiseControlCustomMetricsReporterTopicMinInSync() {
        int minInSync = 1;

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSync
                        ))
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        // The metrics reporter is not configured in a controller only node
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync)));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS, false);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));
    }

    @Test
    public void testCruiseControlWithSingleNodeKafka() {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .withNewCruiseControl()
                        .endCruiseControl()
                    .endSpec()
                    .build();
            KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                    .editSpec()
                        .withReplicas(1)
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });

        assertThat(ex.getMessage(), is("Kafka " + NAMESPACE + "/" + CLUSTER + " has invalid configuration. " +
                "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers."));
    }

    @Test
    public void testCruiseControlWithMinISRGreaterThanReplicas() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), 3,
                                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue(), 2))
                    .endKafka()
                .withNewCruiseControl()
                .endCruiseControl()
                .endSpec()
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });
    }

    @Test
    public void testCruiseControlWithMinISRGreaterThanDefaultReplicas() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), 2))
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });
    }

    @Test
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetricsConfig(metrics)
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertThat(kc.metrics(), is(notNullValue()));
        assertThat(((JmxPrometheusExporterModel) kc.metrics()).getConfigMapName(), is("my-metrics-configuration"));
        assertThat(((JmxPrometheusExporterModel) kc.metrics()).getConfigMapKey(), is("config.yaml"));
    }

    @Test
    public void testMetricsParsingNoMetrics() {
        assertThat(KC.metrics(), is(nullValue()));
    }

    @Test
    public void testInvalidVersion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("6.6.6")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 6.6.6. Supported versions are:"));
    }

    @Test
    public void testUnsupportedVersion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("2.6.0")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.6.0. Supported versions are:"));
    }

    @Test
    public void testInvalidVersionWithCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("2.6.0")
                        .withImage("my-custom/image:latest")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.6.0. Supported versions are:"));
    }

    @Test
    public void testKRaftMetadataVersionValidation()    {
        // Valid values
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.6"));
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.6-IV2"));

        // Minimum supported versions
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.3"));
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.3-IV3"));

        // Invalid Values
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3.6-IV9"));
        assertThat(e.getMessage(), containsString("Metadata version 3.6-IV9 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3"));
        assertThat(e.getMessage(), containsString("Metadata version 3 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3.2"));
        assertThat(e.getMessage(), containsString("Metadata version 3.2 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3.2-IV0"));
        assertThat(e.getMessage(), containsString("Metadata version 3.2-IV0 is invalid"));
    }

    @Test
    public void testKRaftMetadataVersionValidationInFromCrd()   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetadataVersion("3.6-IV9")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class,
                () -> KafkaCluster.fromCrd(
                        Reconciliation.DUMMY_RECONCILIATION,
                        kafka,
                        POOLS,
                        VERSIONS,
                        new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, "3.6-IV9"),
                        null,
                        SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT));
        assertThat(ex.getMessage(), containsString("Metadata version 3.6-IV9 is invalid"));
    }

    @Test
    public void testCustomKRaftMetadataVersion()   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetadataVersion("3.5-IV1")
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                POOLS,
                VERSIONS,
                new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, "3.5-IV1"),
                null,
                SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertThat(kc.getMetadataVersion(), is("3.5-IV1"));
    }

    @Test
    public void testNodePoolForNodeId()   {
        // Existing node
        assertThat(KC.nodePoolForNodeId(1).poolName, is("controllers"));
        assertThat(KC.nodePoolForNodeId(3).poolName, is("mixed"));
        assertThat(KC.nodePoolForNodeId(6).poolName, is("brokers"));

        // Non-existing node
        KafkaCluster.NodePoolNotFoundException e = assertThrows(KafkaCluster.NodePoolNotFoundException.class, () -> KC.nodePoolForNodeId(1874));
        assertThat(e.getMessage(), is("Node ID 1874 does not belong to any known node pool!"));
    }

    @Test
    public void testNodesAndStatuses()  {
        Set<NodeRef> nodes = KC.nodes();
        assertThat(nodes.size(), is(8));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER + "-controllers-0", 0, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-1", 1, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-2", 2, "controllers", true, false),
                new NodeRef(CLUSTER + "-mixed-3", 3, "mixed", true, true),
                new NodeRef(CLUSTER + "-mixed-4", 4, "mixed", true, true),
                new NodeRef(CLUSTER + "-brokers-5", 5, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-6", 6, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-7", 7, "brokers", false, true)));

        Set<NodeRef> brokerNodes = KC.brokerNodes();
        assertThat(brokerNodes.size(), is(5));
        assertThat(brokerNodes, hasItems(new NodeRef(CLUSTER + "-mixed-3", 3, "mixed", true, true),
                new NodeRef(CLUSTER + "-mixed-4", 4, "mixed", true, true),
                new NodeRef(CLUSTER + "-brokers-5", 5, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-6", 6, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-7", 7, "brokers", false, true)));

        Set<NodeRef> controllerNodes = KC.controllerNodes();
        assertThat(controllerNodes.size(), is(5));
        assertThat(controllerNodes, hasItems(new NodeRef(CLUSTER + "-controllers-0", 0, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-1", 1, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-2", 2, "controllers", true, false),
                new NodeRef(CLUSTER + "-mixed-3", 3, "mixed", true, true),
                new NodeRef(CLUSTER + "-mixed-4", 4, "mixed", true, true)));

        Map<String, KafkaNodePoolStatus> statuses = KC.nodePoolStatuses();
        assertThat(statuses.size(), is(3));
        assertThat(statuses.get("controllers").getReplicas(), is(3));
        assertThat(statuses.get("controllers").getLabelSelector(), is("strimzi.io/cluster=foo,strimzi.io/name=foo-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=controllers"));
        assertThat(statuses.get("controllers").getNodeIds().size(), is(3));
        assertThat(statuses.get("controllers").getNodeIds(), hasItems(0, 1, 2));
        assertThat(statuses.get("controllers").getRoles().size(), is(1));
        assertThat(statuses.get("controllers").getRoles(), hasItems(ProcessRoles.CONTROLLER));
        assertThat(statuses.get("mixed").getReplicas(), is(2));
        assertThat(statuses.get("mixed").getLabelSelector(), is("strimzi.io/cluster=foo,strimzi.io/name=foo-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=mixed"));
        assertThat(statuses.get("mixed").getNodeIds().size(), is(2));
        assertThat(statuses.get("mixed").getNodeIds(), hasItems(3, 4));
        assertThat(statuses.get("mixed").getRoles().size(), is(2));
        assertThat(statuses.get("mixed").getRoles(), hasItems(ProcessRoles.CONTROLLER, ProcessRoles.BROKER));
        assertThat(statuses.get("brokers").getReplicas(), is(3));
        assertThat(statuses.get("brokers").getLabelSelector(), is("strimzi.io/cluster=foo,strimzi.io/name=foo-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=brokers"));
        assertThat(statuses.get("brokers").getNodeIds().size(), is(3));
        assertThat(statuses.get("brokers").getNodeIds(), hasItems(5, 6, 7));
        assertThat(statuses.get("brokers").getRoles().size(), is(1));
        assertThat(statuses.get("brokers").getRoles(), hasItems(ProcessRoles.BROKER));
    }

}

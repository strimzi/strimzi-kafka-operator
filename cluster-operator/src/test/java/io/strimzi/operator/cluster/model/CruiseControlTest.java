/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.strimzi.api.kafka.model.common.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.BrokerCapacity;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.cluster.model.cruisecontrol.CpuCapacity;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.strimzi.operator.cluster.model.CruiseControl.API_HEALTHCHECK_PATH;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_ADMIN_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_AUTH_FILE_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.API_USER_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters.DEFAULT_GOALS_CONFIG_KEY;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity"
})
@ParallelSuite
public class CruiseControlTest {
    private final static Set<NodeRef> NODES = Set.of(
            new NodeRef("foo-kafka-0", 0, "kafka", false, true),
            new NodeRef("foo-kafka-1", 1, "kafka", false, true),
            new NodeRef("foo-kafka-2", 2, "kafka", false, true));

    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final String NAMESPACE = "test";
    private static final String CLUSTER = "foo";
    private static final int REPLICAS = 1;
    private static final String IMAGE = "my-image:latest";
    private static final int HEALTH_DELAY = 120;
    private static final int HEALTH_TIMEOUT = 30;
    private static final String REPLICATION_FACTOR = "3";
    private static final String MIN_INSYNC_REPLICAS = "2";
    private static final String BROKER_CAPACITY_CPU = "6.0";
    private static final String BROKER_CAPACITY_OVERRIDE_CPU = "2.0";
    private static final String RESOURCE_LIMIT_CPU = "3.0";
    private static final String RESOURCE_REQUESTS_CPU = "4.0";

    private final Map<String, Object> kafkaConfig = new HashMap<>() {{
            put(CruiseControl.MIN_INSYNC_REPLICAS, MIN_INSYNC_REPLICAS);
            put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR);
        }};

    private final Storage kafkaStorage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String ccImage = "my-cruise-control-image";

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
            .withImage(ccImage)
            .withNewTemplate()
                .withNewPod()
                    .withTmpDirSizeLimit("100Mi")
                .endPod()
            .endTemplate()
            .build();

    private final Kafka kafka = createKafka(cruiseControlSpec);
    private final CruiseControl cc = createCruiseControl(kafka);

    private Map<String, Storage> createStorageMap(Kafka kafkaAssembly) {
        Map<String, Storage> storage = new HashMap<>();
        storage.put("kafka", kafkaAssembly.getSpec().getKafka().getStorage());

        return storage;
    }

    private Map<String, ResourceRequirements> createResourceRequirementsMap(Kafka kafkaAssembly) {
        Map<String, ResourceRequirements> resources = new HashMap<>();
        resources.put("kafka", kafkaAssembly.getSpec().getKafka().getResources());

        return resources;
    }

    private CruiseControl createCruiseControl(Kafka kafkaAssembly) {
        return CruiseControl
                .fromCrd(
                        Reconciliation.DUMMY_RECONCILIATION,
                        kafkaAssembly,
                        VERSIONS,
                        NODES,
                        createStorageMap(kafkaAssembly),
                        createResourceRequirementsMap(kafkaAssembly),
                        SHARED_ENV_PROVIDER
                );
    }

    private Kafka createKafka(CruiseControlSpec cruiseControlSpec) {
        return new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, CLUSTER, REPLICAS, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT))
                .editSpec()
                    .editKafka()
                        .withConfig(kafkaConfig)
                        .withStorage(kafkaStorage)
                    .endKafka()
                    .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();
    }

    private Map<String, String> expectedLabels(String name) {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, CruiseControl.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, CruiseControl.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, CLUSTER,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + CLUSTER,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels() {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels() {
        return expectedLabels(CruiseControlResources.componentName(CLUSTER));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED).withValue(Boolean.toString(CruiseControl.DEFAULT_CRUISE_CONTROL_METRICS_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(CLUSTER) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_SSL_ENABLED).withValue(Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SSL_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_AUTH_ENABLED).withValue(Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SECURITY_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_USER).withValue(CruiseControlApiProperties.API_USER_NAME).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_PORT).withValue(Integer.toString(CruiseControl.REST_API_PORT)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_HEALTHCHECK_PATH).withValue(API_HEALTHCHECK_PATH).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_KAFKA_HEAP_OPTS).withValue("-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS).build());
        return expected;
    }

    private static boolean isJBOD(Object diskCapacity) {
        return diskCapacity instanceof JsonObject;
    }

    @ParallelTest
    public void testBrokerCapacities() {
        // Test user defined capacities
        String userDefinedCpuCapacity = "2575m";
        JsonObject expectedCpuCapacity = new CpuCapacity(userDefinedCpuCapacity).getJson();

        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity userDefinedBrokerCapacity = new io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity();
        userDefinedBrokerCapacity.setCpu(userDefinedCpuCapacity);
        userDefinedBrokerCapacity.setInboundNetwork("50000KB/s");
        userDefinedBrokerCapacity.setOutboundNetwork("50000KB/s");

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withBrokerCapacity(userDefinedBrokerCapacity)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        // Test generated disk capacity
        JbodStorage jbodStorage = new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize("50Gi").build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).build()
                ).build();

        Map<String, Quantity> requests = Map.of(Capacity.RESOURCE_TYPE, new Quantity("400m"));
        Map<String, Quantity> limits = Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"));

        resource = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, CLUSTER, REPLICAS, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT))
            .editSpec()
                .editKafka()
                    .withVersion(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION)
                    .withConfig(kafkaConfig)
                    .withStorage(jbodStorage)
                    .withResources(new ResourceRequirementsBuilder().withRequests(requests).withLimits(limits).build())
                .endKafka()
                .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();

        CruiseControl cc = createCruiseControl(resource);
        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));

        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);
        for (Object brokerEntry : brokerEntries) {
            JsonObject brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY);
            Object diskCapacity = brokerCapacity.getValue(Capacity.DISK_KEY);
            JsonObject cpuCapacity  = brokerCapacity.getJsonObject(Capacity.CPU_KEY);

            assertThat(isJBOD(diskCapacity), is(true));
            assertThat(cpuCapacity, is(expectedCpuCapacity));
        }

        // Test capacity overrides
        String userDefinedCpuCapacityOverride0 = "1.222";
        String inboundNetwork = "50000KB/s";
        String inboundNetworkOverride0 = "25000KB/s";
        String inboundNetworkOverride1 = "10000KiB/s";
        String outboundNetworkOverride1 = "15000KB/s";

        int broker0 = 0;
        int broker1 = 1;
        int broker2 = 2;

        List<Integer> overrideList0 = List.of(broker0, broker1, broker2, broker0);
        List<Integer> overrideList1 = List.of(broker1);

        cruiseControlSpec = new CruiseControlSpecBuilder()
            .withImage(ccImage)
            .withNewBrokerCapacity()
                .withCpu(userDefinedCpuCapacity)
                .withInboundNetwork(inboundNetwork)
                .addNewOverride()
                    .withBrokers(overrideList0)
                    .withCpu(userDefinedCpuCapacityOverride0)
                    .withInboundNetwork(inboundNetworkOverride0)
                .endOverride()
                .addNewOverride()
                    .withBrokers(overrideList1)
                    .withInboundNetwork(inboundNetworkOverride1)
                    .withOutboundNetwork(outboundNetworkOverride1)
                .endOverride()
            .endBrokerCapacity()
            .build();

        resource = createKafka(cruiseControlSpec);
        cc = createCruiseControl(resource);
        configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));

        capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);

        for (Object brokerEntry : brokerEntries) {
            JsonObject brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY);
            Object diskCapacity = brokerCapacity.getValue(Capacity.DISK_KEY);

            assertThat(isJBOD(diskCapacity), is(false));
        }

        JsonObject brokerEntry0 = brokerEntries.getJsonObject(broker0).getJsonObject(Capacity.CAPACITY_KEY);
        assertThat(brokerEntry0.getJsonObject(Capacity.CPU_KEY), is(new CpuCapacity(userDefinedCpuCapacityOverride0).getJson()));
        assertThat(brokerEntry0.getString(Capacity.INBOUND_NETWORK_KEY), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
        assertThat(brokerEntry0.getString(Capacity.OUTBOUND_NETWORK_KEY), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        // When the same broker id is specified in brokers list of multiple overrides, use the value specified in the first override.
        JsonObject brokerEntry1 = brokerEntries.getJsonObject(broker1).getJsonObject(Capacity.CAPACITY_KEY);
        assertThat(brokerEntry1.getJsonObject(Capacity.CPU_KEY), is(new CpuCapacity(userDefinedCpuCapacityOverride0).getJson()));
        assertThat(brokerEntry1.getString(Capacity.INBOUND_NETWORK_KEY), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
        assertThat(brokerEntry1.getString(Capacity.OUTBOUND_NETWORK_KEY), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        JsonObject brokerEntry2 = brokerEntries.getJsonObject(broker2).getJsonObject(Capacity.CAPACITY_KEY);
        assertThat(brokerEntry1.getJsonObject(Capacity.CPU_KEY), is(new CpuCapacity(userDefinedCpuCapacityOverride0).getJson()));
        assertThat(brokerEntry1.getString(Capacity.INBOUND_NETWORK_KEY), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));

        // Test generated CPU capacity
        userDefinedCpuCapacity = "500m";

        requests = Map.of(Capacity.RESOURCE_TYPE, new Quantity(userDefinedCpuCapacity));
        limits = Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"));

        resource = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, CLUSTER, REPLICAS, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT))
            .editSpec()
                .editKafka()
                .withVersion(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION)
                .withStorage(jbodStorage)
                .withResources(new ResourceRequirementsBuilder().withRequests(requests).withLimits(limits).build())
                .withConfig(kafkaConfig)
            .endKafka()
            .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();

        cc = createCruiseControl(resource);
        configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));

        expectedCpuCapacity = new CpuCapacity(userDefinedCpuCapacityOverride0).getJson();

        brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);
        for (Object brokerEntry : brokerEntries) {
            JsonObject brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY);
            JsonObject cpuCapacity  = brokerCapacity.getJsonObject(Capacity.CPU_KEY);
            assertThat(cpuCapacity, is(expectedCpuCapacity));
        }
    }

    @ParallelTest
    public void testBrokerCapacitiesWithPools() {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("foo-pool1-0", 0, "pool1", false, true),
                new NodeRef("foo-pool1-1", 1, "pool1", false, true),
                new NodeRef("foo-pool1-2", 2, "pool1", false, true),
                new NodeRef("foo-pool2-10", 10, "pool2", false, true),
                new NodeRef("foo-pool2-11", 11, "pool2", false, true),
                new NodeRef("foo-pool2-12", 12, "pool2", false, true)
        );

        Map<String, Storage> storage = new HashMap<>();
        storage.put("pool1", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());
        storage.put("pool2", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(1).withSize("1Ti").build()).build());

        Map<String, ResourceRequirements> resources = new HashMap<>();
        resources.put("pool1", new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build());
        resources.put("pool2", new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("5"), "memory", new Quantity("20Gi"))).build());

        // Test the capacity
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, nodes, storage, resources, SHARED_ENV_PROVIDER);
        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);

        assertThat(brokerEntries.size(), is(6));

        // Broker 0
        JsonObject brokerEntry = brokerEntries.getJsonObject(0);
        assertThat(brokerEntry.getInteger("brokerId"), is(0));
        JsonObject brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("4.0"));
        JsonObject brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-0/kafka-log0"), is("102400.0"));

        // Broker 1
        brokerEntry = brokerEntries.getJsonObject(1);
        assertThat(brokerEntry.getInteger("brokerId"), is(1));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("4.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-0/kafka-log1"), is("102400.0"));

        // Broker 2
        brokerEntry = brokerEntries.getJsonObject(2);
        assertThat(brokerEntry.getInteger("brokerId"), is(2));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("4.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-0/kafka-log2"), is("102400.0"));

        // Broker 10
        brokerEntry = brokerEntries.getJsonObject(3);
        assertThat(brokerEntry.getInteger("brokerId"), is(10));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("5.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-1/kafka-log10"), is("1048576.0"));

        // Broker 11
        brokerEntry = brokerEntries.getJsonObject(4);
        assertThat(brokerEntry.getInteger("brokerId"), is(11));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("5.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-1/kafka-log11"), is("1048576.0"));

        // Broker 12
        brokerEntry = brokerEntries.getJsonObject(5);
        assertThat(brokerEntry.getInteger("brokerId"), is(12));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("5.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-1/kafka-log12"), is("1048576.0"));
    }

    @ParallelTest
    public void testFromConfigMap() {
        assertThat(cc.namespace, is(NAMESPACE));
        assertThat(cc.cluster, is(CLUSTER));
        assertThat(cc.getImage(), is(ccImage));
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        assertThat(dep.getMetadata().getName(), is(CruiseControlResources.componentName(CLUSTER)));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
        TestUtils.checkOwnerReference(dep, kafka);

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(15));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(5));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(15));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(5));

        assertThat(ccContainer.getEnv(), is(getExpectedEnvVars()));
        assertThat(ccContainer.getPorts().size(), is(1));
        assertThat(ccContainer.getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(ccContainer.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(5));

        Volume volume = volumes.stream().filter(vol -> CruiseControl.TLS_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControlResources.secretName(CLUSTER)));

        volume = volumes.stream().filter(vol -> CruiseControl.TLS_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(AbstractModel.clusterCaCertSecretName(CLUSTER)));

        volume = volumes.stream().filter(vol -> CruiseControl.API_AUTH_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControlResources.apiSecretName(CLUSTER)));

        volume = volumes.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("100Mi")));

        volume = volumes.stream().filter(vol -> CruiseControl.CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getConfigMap().getName(), is(CruiseControlResources.configMapName(CLUSTER)));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(5));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_CC_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.API_AUTH_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.API_AUTH_CONFIG_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.CONFIG_VOLUME_MOUNT));
    }

    @ParallelTest
    public void testEnvVars() {
        assertThat(cc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = cc.generateDeployment(Map.of(), true, ImagePullPolicy.ALWAYS, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = cc.generateDeployment(Map.of(), true, ImagePullPolicy.IFNOTPRESENT, null);
        containers = dep.getSpec().getTemplate().getSpec().getContainers();
        ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testContainerTemplateEnvVars() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withEnv(envVar1, envVar2)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = createCruiseControl(resource);
        List<EnvVar> envVarList = cc.getEnvVars();

        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));
    }

    @ParallelTest
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withEnv(envVar1, envVar2)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = createCruiseControl(resource);
        List<EnvVar> envVarList = cc.getEnvVars();

        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));
    }

    @ParallelTest
    public void testCruiseControlNotDeployed() {
        Kafka kafka = createKafka(null);
        assertThat(createCruiseControl(kafka), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateService() {
        Service svc = cc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(CruiseControlResources.serviceName(CLUSTER))));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(CruiseControl.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        TestUtils.checkOwnerReference(svc, kafka);
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> saLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> saAnots = TestUtils.map("a7", "v7", "a8", "v8");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key1")
                                    .withOperator("In")
                                    .withValues("value1", "value2")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> tolerations = singletonList(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewDeployment()
                        .withNewMetadata()
                            .withLabels(depLabels)
                            .withAnnotations(depAnots)
                        .endMetadata()
                    .endDeployment()
                .withNewPod()
                    .withNewMetadata()
                        .withLabels(podLabels)
                        .withAnnotations(podAnots)
                    .endMetadata()
                    .withPriorityClassName("top-priority")
                    .withSchedulerName("my-scheduler")
                    .withHostAliases(hostAlias1, hostAlias2)
                    .withAffinity(affinity)
                    .withTolerations(tolerations)
                .endPod()
                .withNewApiService()
                    .withNewMetadata()
                        .withLabels(svcLabels)
                        .withAnnotations(svcAnots)
                    .endMetadata()
                    .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                .endApiService()
                .withNewServiceAccount()
                    .withNewMetadata()
                        .withLabels(saLabels)
                        .withAnnotations(saAnots)
                    .endMetadata()
                .endServiceAccount()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = createCruiseControl(resource);

        // Check Deployment
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        depLabels.putAll(expectedLabels());
        assertThat(dep.getMetadata().getLabels(), is(depLabels));
        assertThat(dep.getMetadata().getAnnotations(), is(depAnots));

        // Check Pods
        podLabels.putAll(expectedLabels());
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(podLabels));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations(), is(podAnots));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));

        // Check Service
        svcLabels.putAll(expectedLabels());
        Service svc = cc.generateService();
        assertThat(svc.getMetadata().getLabels(), is(svcLabels));
        assertThat(svc.getMetadata().getAnnotations(), is(svcAnots));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Service Account
        ServiceAccount sa = cc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();

        assertThat(ccContainer.getResources().getLimits(), is(limits));
        assertThat(ccContainer.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testCpuCapacityGeneration() {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("foo-pool1-0", 0, "pool1", false, true),
                new NodeRef("foo-pool1-1", 1, "pool1", false, true),
                new NodeRef("foo-pool1-2", 2, "pool1", false, true)
        );

        Map<String, Storage> storage = new HashMap<>();
        storage.put("pool1", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());

        /* In this test case, when override is set for a broker, it takes precedence over the general broker capacity setting for that
           specific broker, but the general broker capacity takes precedence over the Kafka resource request */
        ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder()
                .addToLimits("cpu", new Quantity(RESOURCE_LIMIT_CPU))
                .addToRequests("cpu", new Quantity(RESOURCE_REQUESTS_CPU))
                .build();

        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacityOne = new BrokerCapacityBuilder()
                .withCpu(BROKER_CAPACITY_CPU)
                .withOverrides()
                    .addNewOverride()
                        .addToBrokers(0)
                        .withCpu(BROKER_CAPACITY_OVERRIDE_CPU)
                    .endOverride()
                .build();

        verifyBrokerCapacity(nodes, storage, resourceRequirements, brokerCapacityOne, BROKER_CAPACITY_OVERRIDE_CPU, BROKER_CAPACITY_CPU, BROKER_CAPACITY_CPU);

        // In this test case, when override is set for a broker, it takes precedence over the Kafka resource request
        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacityTwo = new BrokerCapacityBuilder()
                .withOverrides()
                    .addNewOverride()
                        .addToBrokers(0)
                        .withCpu(BROKER_CAPACITY_OVERRIDE_CPU)
                    .endOverride()
                .build();

        verifyBrokerCapacity(nodes, storage, resourceRequirements, brokerCapacityTwo, BROKER_CAPACITY_OVERRIDE_CPU, RESOURCE_REQUESTS_CPU, RESOURCE_REQUESTS_CPU);

        /* In this test case, when neither the override nor the CPU resource request are configured but the CPU
           resource limit for CPU is set for the Kafka brokers; therefore, resource limit will be used as the CPU capacity */
        ResourceRequirements resourceRequirements3 = new ResourceRequirementsBuilder()
                .addToLimits("cpu", new Quantity(RESOURCE_LIMIT_CPU))
                .build();

        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacityThree = new BrokerCapacityBuilder()
                .build();

        verifyBrokerCapacity(nodes, storage, resourceRequirements3, brokerCapacityThree, RESOURCE_LIMIT_CPU, RESOURCE_LIMIT_CPU, RESOURCE_LIMIT_CPU);

        /* In this test case, when neither the override nor the Kafka resource requests or limits for CPU are configured,
           the CPU capacity will be set to DEFAULT_CPU_CORE_CAPACITY */
        ResourceRequirements resourceRequirements2 = new ResourceRequirementsBuilder()
                .build();

        verifyBrokerCapacity(nodes, storage, resourceRequirements2, brokerCapacityThree, BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY, BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY, BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY);
    }

    private void verifyBrokerCapacity(Set<NodeRef> nodes,
                                     Map<String, Storage> storage,
                                     ResourceRequirements resourceRequirements,
                                     io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacity,
                                     String brokerOneCpuValue,
                                     String brokerTwoCpuValue,
                                     String brokerThreeCpuValue) {

        HashMap<String, ResourceRequirements> resources = new HashMap<>();
        resources.put("pool1", resourceRequirements);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withBrokerCapacity(brokerCapacity)
                .build();

        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withResources(resourceRequirements)
                        .withConfig(kafkaConfig)
                    .endKafka()
                    .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, nodes, storage, resources, SHARED_ENV_PROVIDER);
        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);
        assertThat(brokerEntries.getJsonObject(0).getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY).getString("num.cores"), is(Matchers.equalTo(brokerOneCpuValue)));
        assertThat(brokerEntries.getJsonObject(1).getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY).getString("num.cores"), is(Matchers.equalTo(brokerTwoCpuValue)));
        assertThat(brokerEntries.getJsonObject(2).getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY).getString("num.cores"), is(Matchers.equalTo(brokerThreeCpuValue)));
    }

    @ParallelTest
    public void testApiSecurity() {
        // Test with security enabled
        testApiSecurity(true, true);

        // Test with security disabled
        testApiSecurity(false, false);
    }

    public void testApiSecurity(Boolean apiAuthEnabled, Boolean apiSslEnabled) {
        String e1Key = CruiseControl.ENV_VAR_API_AUTH_ENABLED;
        String e1Value = apiAuthEnabled.toString();
        EnvVar e1 = new EnvVar(e1Key, e1Value, null);

        String e2Key = CruiseControl.ENV_VAR_API_SSL_ENABLED;
        String e2Value = apiSslEnabled.toString();
        EnvVar e2 = new EnvVar(e2Key, e2Value, null);

        Map<String, Object> config = new HashMap<>();
        config.put(CruiseControlConfigurationParameters.WEBSERVER_SECURITY_ENABLE.getValue(), apiAuthEnabled);
        config.put(CruiseControlConfigurationParameters.WEBSERVER_SSL_ENABLE.getValue(), apiSslEnabled);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(config)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        List<EnvVar> envVarList = ccContainer.getEnv();

        assertThat(envVarList.contains(e1),  is(true));
        assertThat(envVarList.contains(e2),  is(true));
    }

    @ParallelTest
    public void testProbeConfiguration()   {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewLivenessProbe()
                        .withInitialDelaySeconds(HEALTH_DELAY)
                        .withTimeoutSeconds(HEALTH_TIMEOUT)
                .endLivenessProbe()
                .withNewReadinessProbe()
                       .withInitialDelaySeconds(HEALTH_DELAY)
                       .withTimeoutSeconds(HEALTH_TIMEOUT)
                .endReadinessProbe()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(HEALTH_DELAY));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(HEALTH_TIMEOUT));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(HEALTH_DELAY));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(HEALTH_TIMEOUT));
    }

    @ParallelTest
    public void testSecurityContext() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewPod()
                        .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                    .endPod()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        CruiseControl cc = createCruiseControl(createKafka(cruiseControlSpec));
        cc.securityProvider = new RestrictedPodSecurityProvider();
        cc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
    }

    @ParallelTest
    public void testJvmOptions() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withNewJvmOptions()
                .withXms("128m")
                .withXmx("256m")
                .withXx(Map.of("InitiatingHeapOccupancyPercent", "36"))
                .withJavaSystemProperties(new SystemPropertyBuilder().withName("myProperty").withValue("myValue").build(),
                        new SystemPropertyBuilder().withName("myProperty2").withValue("myValue2").build())
                .endJvmOptions()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);

        EnvVar systemProps = cc.getEnvVars().stream().filter(var -> AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES.equals(var.getName())).findFirst().orElse(null);
        assertThat(systemProps, is(notNullValue()));
        assertThat(systemProps.getValue(), containsString("-DmyProperty=myValue"));
        assertThat(systemProps.getValue(), containsString("-DmyProperty2=myValue2"));

        EnvVar heapOpts = cc.getEnvVars().stream().filter(var -> AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS.equals(var.getName())).findFirst().orElse(null);
        assertThat(heapOpts, is(notNullValue()));
        assertThat(heapOpts.getValue(), containsString("-Xms128m"));
        assertThat(heapOpts.getValue(), containsString("-Xmx256m"));

        EnvVar perfOptions = cc.getEnvVars().stream().filter(var -> AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS.equals(var.getName())).findFirst().orElse(null);
        assertThat(perfOptions, is(notNullValue()));
        assertThat(perfOptions.getValue(), containsString("-XX:InitiatingHeapOccupancyPercent=36"));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testCruiseControlContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withSecurityContext(securityContext)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(CruiseControl.CRUISE_CONTROL_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicy() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        NetworkPolicy np = cc.generateNetworkPolicy("operator-namespace", null, false);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicyInTheSameNamespace() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();
        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeerBuilder()
            .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, format("%s-entity-operator", CLUSTER)))
            .endPodSelector()
            .build();

        NetworkPolicy np = cc.generateNetworkPolicy(NAMESPACE, null, true);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(2));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
        assertThat(rules.contains(entityOperatorPeer), is(true));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicyWithNamespaceLabels() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector()
                    .withMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"))
                .endNamespaceSelector()
                .build();
        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeerBuilder()
            .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, format("%s-entity-operator", CLUSTER)))
            .endPodSelector()
            .withNewNamespaceSelector()
                .withMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"))
            .endNamespaceSelector()
            .build();

        NetworkPolicy np = cc.generateNetworkPolicy(null, Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")), true);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(2));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
        assertThat(rules.contains(entityOperatorPeer), is(true));
    }

    @ParallelTest
    public void testGoalsCheck() {

        String customGoals = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal";

        Map<String, Object> customGoalConfig = new HashMap<>();
        customGoalConfig.put(DEFAULT_GOALS_CONFIG_KEY.getValue(), customGoals);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(customGoalConfig)
                .build();

        Kafka resourceWithCustomGoals = createKafka(cruiseControlSpec);

        CruiseControl cruiseControlWithCustomGoals = createCruiseControl(resourceWithCustomGoals);

        String anomalyDetectionGoals =  cruiseControlWithCustomGoals
                .configuration.asOrderedProperties().asMap()
                .get(ANOMALY_DETECTION_CONFIG_KEY.getValue());

        assertThat(anomalyDetectionGoals, is(customGoals));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withMetricsConfig(metrics)
                .build();

        Kafka kafkaAssembly = createKafka(cruiseControlSpec);
        CruiseControl cc = createCruiseControl(kafkaAssembly);

        assertThat(cc.metrics().isEnabled(), is(true));
        assertThat(cc.metrics().getConfigMapName(), is("my-metrics-configuration"));
        assertThat(cc.metrics().getConfigMapKey(), is("config.yaml"));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder().build();
        Kafka kafkaAssembly = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(kafkaAssembly);

        assertThat(cc.metrics().isEnabled(), is(false));
        assertThat(cc.metrics().getConfigMapName(), is(nullValue()));
        assertThat(cc.metrics().getConfigMapKey(), is(nullValue()));
    }

    @ParallelTest
    public void testDefaultTopicNames() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_PARTITION_METRIC_TOPIC_NAME);
        topicConfigs.put(CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_BROKER_METRIC_TOPIC_NAME);
        topicConfigs.put(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = createCruiseControl(resource);
        topicConfigs.forEach((configParam, name) -> assertThat(cc.configuration.getConfiguration(), containsString(String.format("%s=%s", configParam, name))));
    }

    @ParallelTest
    public void testCustomTopicNames() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), "partition-topic");
        topicConfigs.put(CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), "broker-topic");
        topicConfigs.put(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), "metric-reporter-topic");
        Map<String, Object> customConfig = new HashMap<>();
        customConfig.putAll(topicConfigs);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(customConfig)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = createCruiseControl(resource);
        topicConfigs.forEach((configParam, name) -> assertThat(cc.configuration.getConfiguration(), containsString(String.format("%s=%s", configParam, name))));
    }

    private Properties getCcProperties(Kafka resource) {
        CruiseControl cc = createCruiseControl(resource);
        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        return parsePropertiesString(configMap.getData().get(CruiseControl.SERVER_CONFIG_FILENAME));
    }

    private static Properties parsePropertiesString(String kafkaPropertiesString) {
        Properties properties = new Properties();
        try (StringReader reader = new StringReader(kafkaPropertiesString)) {
            properties.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    @ParallelTest
    public void testSampleStoreTopicReplicationFactorConfig() {
        // Test that the replication factor of Cruise Control's sample store topic is set to Kafka cluster's `default.replication.factor`
        // when not explicitly set in Cruise Control config
        Properties properties = getCcProperties(kafka);
        assertThat(properties.getProperty(CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue()), is(REPLICATION_FACTOR));

        // Test that the replication factor of Cruise Control's sample store topic is set to value set in Cruise Control config
        String replicationFactor = "1";
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(Map.of(CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue(), replicationFactor))
                .build();

        properties = getCcProperties(createKafka(cruiseControlSpec));
        assertThat(properties.getProperty(CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue()), is(replicationFactor));
    }

    @ParallelTest
    public void testGenerateApiSecret() {
        PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
        var newSecret = cc.generateApiSecret(passwordGenerator, null, null);
        assertThat(newSecret, is(notNullValue()));
        assertThat(newSecret.getData(), is(notNullValue()));
        assertThat(newSecret.getData().size(), is(3));
        assertThat(newSecret.getData().get(API_ADMIN_PASSWORD_KEY), is(notNullValue()));
        assertThat(newSecret.getData().get(API_USER_PASSWORD_KEY), is(notNullValue()));
        assertThat(newSecret.getData().get(API_AUTH_FILE_KEY), is(notNullValue()));
        assertTrue(Util.decodeFromBase64(newSecret.getData().get(API_AUTH_FILE_KEY)).contains("admin"));
        assertTrue(Util.decodeFromBase64(newSecret.getData().get(API_AUTH_FILE_KEY)).contains("user"));

        var secretWithAdditionalUser = cc.generateApiSecret(passwordGenerator, null, 
            new CruiseControl.CruiseControlUser("foo", "changeit"));
        assertThat(secretWithAdditionalUser, is(notNullValue()));
        assertThat(secretWithAdditionalUser.getData(), is(notNullValue()));
        assertThat(secretWithAdditionalUser.getData().size(), is(3));
        assertThat(secretWithAdditionalUser.getData().get(API_ADMIN_PASSWORD_KEY), is(notNullValue()));
        assertThat(secretWithAdditionalUser.getData().get(API_USER_PASSWORD_KEY), is(notNullValue()));
        assertThat(secretWithAdditionalUser.getData().get(API_AUTH_FILE_KEY), is(notNullValue()));
        assertTrue(Util.decodeFromBase64(secretWithAdditionalUser.getData().get(API_AUTH_FILE_KEY)).contains("foo"));
        assertTrue(Util.decodeFromBase64(secretWithAdditionalUser.getData().get(API_AUTH_FILE_KEY)).contains("changeit"));

        var password = Util.encodeToBase64("changeit");
        var auth = Util.encodeToBase64("admin:changeit,ADMIN\nuser:changeit,USER\n");
        var oldSecret = cc.generateApiSecret(passwordGenerator, new SecretBuilder().withData(Map.of(
            API_ADMIN_PASSWORD_KEY, password, API_USER_PASSWORD_KEY, password, API_AUTH_FILE_KEY, auth
        )).build(), null);
        assertThat(oldSecret, is(notNullValue()));
        assertThat(oldSecret.getData(), is(notNullValue()));
        assertThat(oldSecret.getData().size(), is(3));
        assertThat(oldSecret.getData().get(API_ADMIN_PASSWORD_KEY), is(password));
        assertThat(oldSecret.getData().get(API_USER_PASSWORD_KEY), is(password));
        assertThat(oldSecret.getData().get(API_AUTH_FILE_KEY), is(auth));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(API_ADMIN_PASSWORD_KEY, password)).build(), null));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(API_USER_PASSWORD_KEY, password)).build(), null));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(API_AUTH_FILE_KEY, auth)).build(), null));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of()).build(), null));
        
        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(
                API_ADMIN_PASSWORD_KEY, password, API_USER_PASSWORD_KEY, password, API_AUTH_FILE_KEY, 
                    Util.encodeToBase64("admin:changeit,ADMIN\n")
            )).build(), null));
        
        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(
                API_ADMIN_PASSWORD_KEY, password, API_USER_PASSWORD_KEY, password, API_AUTH_FILE_KEY, 
                    Util.encodeToBase64("user:changeit,USER\n")
            )).build(), null));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(
                API_ADMIN_PASSWORD_KEY, " ", API_USER_PASSWORD_KEY, password, API_AUTH_FILE_KEY, auth
            )).build(), null));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(
                API_ADMIN_PASSWORD_KEY, password, API_USER_PASSWORD_KEY, " ", API_AUTH_FILE_KEY, auth
            )).build(), null));

        assertThrows(RuntimeException.class, () -> cc.generateApiSecret(passwordGenerator,
            new SecretBuilder().withData(Map.of(
                API_ADMIN_PASSWORD_KEY, password, API_USER_PASSWORD_KEY, password, API_AUTH_FILE_KEY, " "
            )).build(), null));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
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
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
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
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.BrokerCapacity;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.cluster.model.cruisecontrol.CpuCapacity;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.strimzi.operator.cluster.model.CruiseControl.API_HEALTHCHECK_PATH;
import static io.strimzi.operator.cluster.model.CruiseControl.API_USER_NAME;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_CRUISE_CONTROL_CAPACITY_CONFIGURATION;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters.DEFAULT_GOALS_CONFIG_KEY;
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

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity"
})
@ParallelSuite
public class CruiseControlTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String minInsyncReplicas = "2";

    private final Map<String, Object> kafkaConfig = singletonMap(CruiseControl.MIN_INSYNC_REPLICAS, minInsyncReplicas);
    private final Map<String, Object> ccConfig = new HashMap<>() {{
            putAll(CruiseControlConfiguration.getCruiseControlDefaultPropertiesMap());
            put("num.partition.metrics.windows", "2");
        }};

    private final CruiseControlConfiguration ccConfiguration = new CruiseControlConfiguration(Reconciliation.DUMMY_RECONCILIATION, ccConfig.entrySet());

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
            .withConfig(ccConfig)
            .withNewTemplate()
                .withNewPod()
                    .withTmpDirSizeLimit("100Mi")
                .endPod()
            .endTemplate()
            .build();

    private final Kafka kafka = createKafka(cruiseControlSpec);
    private final CruiseControl cc = createCruiseControl(kafka);

    private CruiseControl createCruiseControl(Kafka kafkaAssembly) {
        return CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS, kafkaAssembly.getSpec().getKafka().getStorage());
    }

    private Kafka createKafka(CruiseControlSpec cruiseControlSpec) {
        return new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withConfig(kafkaConfig)
                        .withStorage(kafkaStorage)
                   .endKafka()
                  .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, CruiseControl.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, CruiseControl.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(CruiseControlResources.deploymentName(cluster));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED).withValue(Boolean.toString(CruiseControl.DEFAULT_CRUISE_CONTROL_METRICS_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_MIN_INSYNC_REPLICAS).withValue(minInsyncReplicas).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_CRUISE_CONTROL_CAPACITY_CONFIGURATION).withValue(cc.capacity.toString()).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_SSL_ENABLED).withValue(Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SSL_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_AUTH_ENABLED).withValue(Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SECURITY_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_USER).withValue(API_USER_NAME).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_PORT).withValue(Integer.toString(CruiseControl.REST_API_PORT)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_HEALTHCHECK_PATH).withValue(API_HEALTHCHECK_PATH).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_KAFKA_HEAP_OPTS).withValue("-Xms" + AbstractModel.DEFAULT_JVM_XMS).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_CONFIGURATION).withValue(ccConfiguration.getConfiguration()).build());

        return expected;
    }

    public String getCapacityConfigurationFromEnvVar(Kafka resource, String envVar) {
        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        List<EnvVar> ccEnvVars = ccContainer.getEnv();

        return ccEnvVars.stream().filter(var -> envVar.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow();
    }

    private static boolean isJBOD(Map<String, Object> brokerCapacity) {
        return brokerCapacity.get(Capacity.DISK_KEY) instanceof JsonObject;
    }

    @ParallelTest
    public void testBrokerCapacities() {
        // Test user defined capacities
        String userDefinedCpuCapacity = "2575m";

        io.strimzi.api.kafka.model.balancing.BrokerCapacity userDefinedBrokerCapacity = new io.strimzi.api.kafka.model.balancing.BrokerCapacity();
        userDefinedBrokerCapacity.setCpu(userDefinedCpuCapacity);
        userDefinedBrokerCapacity.setInboundNetwork("50000KB/s");
        userDefinedBrokerCapacity.setOutboundNetwork("50000KB/s");

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withBrokerCapacity(userDefinedBrokerCapacity)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        Capacity capacity = new Capacity(Reconciliation.DUMMY_RECONCILIATION, resource.getSpec(), kafkaStorage);

        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_CRUISE_CONTROL_CAPACITY_CONFIGURATION), is(capacity.toString()));

        // Test generated disk capacity
        JbodStorage jbodStorage = new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize("50Gi").build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).build()
                ).build();

        Map<String, Quantity> requests = Map.of(Capacity.RESOURCE_TYPE, new Quantity("400m"));
        Map<String, Quantity> limits = Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"));

        resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .editKafka()
                    .withVersion(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION)
                    .withStorage(jbodStorage)
                    .withResources(new ResourceRequirementsBuilder().withRequests(requests).withLimits(limits).build())
                .endKafka()
                .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();
        
        capacity = new Capacity(Reconciliation.DUMMY_RECONCILIATION, resource.getSpec(), jbodStorage);
        String cpuCapacity = new CpuCapacity(userDefinedCpuCapacity).toString();

        JsonArray brokerEntries = capacity.generateCapacityConfig().getJsonArray(Capacity.CAPACITIES_KEY);
        for (Object brokerEntry : brokerEntries) {
            Map<String, Object> brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY).getMap();
            assertThat(isJBOD(brokerCapacity), is(true));
            assertThat(brokerCapacity.get(Capacity.CPU_KEY).toString(), is(cpuCapacity));
        }

        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_CRUISE_CONTROL_CAPACITY_CONFIGURATION), is(capacity.toString()));

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
        capacity = new Capacity(Reconciliation.DUMMY_RECONCILIATION, resource.getSpec(), kafkaStorage);

        brokerEntries = capacity.generateCapacityConfig().getJsonArray(Capacity.CAPACITIES_KEY);
        for (Object brokerEntry : brokerEntries) {
            Map<String, Object> brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY).getMap();
            assertThat(isJBOD(brokerCapacity), is(false));
        }

        TreeMap<Integer, BrokerCapacity> capacityEntries = capacity.getCapacityEntries();
        assertThat(capacityEntries.get(BrokerCapacity.DEFAULT_BROKER_ID).getCpu().toString(), is(new CpuCapacity(userDefinedCpuCapacity).toString()));
        assertThat(capacityEntries.get(BrokerCapacity.DEFAULT_BROKER_ID).getInboundNetwork(), is(Capacity.getThroughputInKiB(inboundNetwork)));
        assertThat(capacityEntries.get(BrokerCapacity.DEFAULT_BROKER_ID).getOutboundNetwork(), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        assertThat(capacityEntries.get(broker0).getInboundNetwork(), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
        assertThat(capacityEntries.get(broker0).getOutboundNetwork(), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        // When the same broker id is specified in brokers list of multiple overrides, use the value specified in the first override.
        assertThat(capacityEntries.get(broker1).getCpu().toString(), is(new CpuCapacity(userDefinedCpuCapacityOverride0).toString()));
        assertThat(capacityEntries.get(broker1).getInboundNetwork(), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
        assertThat(capacityEntries.get(broker1).getOutboundNetwork(), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        assertThat(capacityEntries.get(broker2).getCpu().toString(), is(new CpuCapacity(userDefinedCpuCapacityOverride0).toString()));
        assertThat(capacityEntries.get(broker2).getInboundNetwork(), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));

        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_CRUISE_CONTROL_CAPACITY_CONFIGURATION), is(capacity.toString()));

        // Test generated CPU capacity
        userDefinedCpuCapacity = "500m";

        requests = Map.of(Capacity.RESOURCE_TYPE, new Quantity(userDefinedCpuCapacity));
        limits = Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"));

        resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .editKafka()
                .withVersion(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION)
                .withStorage(jbodStorage)
                .withResources(new ResourceRequirementsBuilder().withRequests(requests).withLimits(limits).build())
            .endKafka()
            .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();

        capacity = new Capacity(Reconciliation.DUMMY_RECONCILIATION, resource.getSpec(), kafkaStorage);
        cpuCapacity = new CpuCapacity(userDefinedCpuCapacity).toString();

        brokerEntries = capacity.generateCapacityConfig().getJsonArray(Capacity.CAPACITIES_KEY);
        for (Object brokerEntry : brokerEntries) {
            Map<String, Object> brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY).getMap();
            assertThat(brokerCapacity.get(Capacity.CPU_KEY).toString(), is(cpuCapacity));
        }
    }

    @ParallelTest
    public void testFromConfigMap() {
        assertThat(cc.namespace, is(namespace));
        assertThat(cc.cluster, is(cluster));
        assertThat(cc.getImage(), is(ccImage));
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Deployment dep = cc.generateDeployment(true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        assertThat(dep.getMetadata().getName(), is(CruiseControlResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        TestUtils.checkOwnerReference(dep, kafka);

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(CruiseControl.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(CruiseControl.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(CruiseControl.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(CruiseControl.DEFAULT_HEALTHCHECK_TIMEOUT));

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
        assertThat(volume.getSecret().getSecretName(), is(CruiseControlResources.secretName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.TLS_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(AbstractModel.clusterCaCertSecretName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getConfigMap().getName(), is(CruiseControlResources.logAndMetricsConfigMapName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.API_AUTH_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControlResources.apiSecretName(cluster)));

        volume = volumes.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("100Mi")));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(5));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_CC_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.API_AUTH_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.API_AUTH_CONFIG_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(cc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = cc.generateDeployment(true, ImagePullPolicy.ALWAYS, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = cc.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT, null);
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
        assertThat(CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, kafka.getSpec().getKafka().getStorage()), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = cc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(CruiseControlResources.serviceName(cluster))));
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
        Deployment dep = cc.generateDeployment(true, null, null);
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
        assertThat(svc.getMetadata().getAnnotations(),  is(svcAnots));
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
        Deployment dep = cc.generateDeployment(true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();

        assertThat(ccContainer.getResources().getLimits(), is(limits));
        assertThat(ccContainer.getResources().getRequests(), is(requests));
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

        Map<String, Object> config = ccConfig;
        config.put(CruiseControlConfigurationParameters.WEBSERVER_SECURITY_ENABLE.getValue(), apiAuthEnabled);
        config.put(CruiseControlConfigurationParameters.WEBSERVER_SSL_ENABLE.getValue(), apiSslEnabled);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(config)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(true, null, null);
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
                        .withInitialDelaySeconds(healthDelay)
                        .withTimeoutSeconds(healthTimeout)
                .endLivenessProbe()
                .withNewReadinessProbe()
                       .withInitialDelaySeconds(healthDelay)
                       .withTimeoutSeconds(healthTimeout)
                .endReadinessProbe()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(healthTimeout));
    }

    @ParallelTest
    public void testSecurityContext() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(ccConfig)
                .withNewTemplate()
                    .withNewPod()
                        .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                    .endPod()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);

        Deployment dep = cc.generateDeployment(true, null, null);
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

        Deployment dep = cc.generateDeployment(true, null, null);
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

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, kafkaStorage);

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
        Deployment dep = cc.generateDeployment(true, null, null);
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
                .withConfig(ccConfig)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withSecurityContext(securityContext)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);

        Deployment dep = cc.generateDeployment(true, null, null);

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

        NetworkPolicy np = cc.generateNetworkPolicy("operator-namespace", null);

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

        NetworkPolicy np = cc.generateNetworkPolicy(namespace, null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
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

        NetworkPolicy np = cc.generateNetworkPolicy(null, Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testGoalsCheck() {

        String customGoals = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal";

        Map<String, Object> customGoalConfig = ccConfig;
        customGoalConfig.put(DEFAULT_GOALS_CONFIG_KEY.getValue(), customGoals);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(customGoalConfig)
                .build();

        Kafka resourceWithCustomGoals = createKafka(cruiseControlSpec);

        CruiseControl cruiseControlWithCustomGoals = createCruiseControl(resourceWithCustomGoals);

        String anomalyDetectionGoals =  cruiseControlWithCustomGoals
                .getConfiguration().asOrderedProperties().asMap()
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

        assertThat(cc.isMetricsEnabled(), is(true));
        assertThat(cc.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder().build();
        Kafka kafkaAssembly = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(kafkaAssembly);

        assertThat(cc.isMetricsEnabled(), is(false));
        assertThat(cc.getMetricsConfigInCm(), is(nullValue()));
    }

    @ParallelTest
    public void testDefaultTopicNames() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(ccConfig)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        List<EnvVar> envVarList = ccContainer.getEnv();
        Optional<EnvVar> configEnvVar = envVarList.stream().filter(envVar -> envVar.getName().equals(CruiseControl.ENV_VAR_CRUISE_CONTROL_CONFIGURATION))
                .findFirst();
        assertThat(configEnvVar.isPresent(), is(true));
        String config = configEnvVar.get().getValue();
        assertThat(config, containsString(String.format("%s=%s", CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_PARTITION_METRIC_TOPIC_NAME)));
        assertThat(config, containsString(String.format("%s=%s", CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_BROKER_METRIC_TOPIC_NAME)));
        assertThat(config, containsString(String.format("%s=%s", CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME)));
    }

    @ParallelTest
    public void testCustomTopicNames() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), "partition-topic");
        topicConfigs.put(CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), "broker-topic");
        topicConfigs.put(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), "metric-reporter-topic");
        Map<String, Object> customConfig = ccConfig;
        customConfig.putAll(topicConfigs);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(customConfig)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = createCruiseControl(resource);
        Deployment dep = cc.generateDeployment(true, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().orElseThrow();
        List<EnvVar> envVarList = ccContainer.getEnv();
        Optional<EnvVar> configEnvVar = envVarList.stream().filter(envVar -> envVar.getName().equals(CruiseControl.ENV_VAR_CRUISE_CONTROL_CONFIGURATION))
                .findFirst();
        assertThat(configEnvVar.isPresent(), is(true));
        String config = configEnvVar.get().getValue();
        topicConfigs.forEach((configParam, name) -> assertThat(config, containsString(String.format("%s=%s", configParam, name))));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}

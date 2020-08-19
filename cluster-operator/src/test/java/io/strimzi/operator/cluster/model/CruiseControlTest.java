/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_DISK_MIB_CAPACITY;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters.CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters.CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_DISK_MIB_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class CruiseControlTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String minInsyncReplicas = "2";
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> kafkaConfig = singletonMap(CruiseControl.MIN_INSYNC_REPLICAS, minInsyncReplicas);
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");

    CruiseControlConfiguration configuration = new CruiseControlConfiguration(new HashMap<String, Object>() {{
            putAll(CruiseControlConfiguration.getCruiseControlDefaultPropertiesMap());
            put("num.partition.metrics.windows", "2");
        }}.entrySet()
    );
    private final Storage kafkaStorage = new EphemeralStorage();
    private final SingleVolumeStorage zkStorage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private final String version = KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;
    private final String ccImage = "my-cruise-control-image";

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
            .withImage(ccImage)
            .withConfig((Map) configuration.asOrderedProperties().asMap())
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .editKafka()
                    .withVersion(version)
                    .withConfig(kafkaConfig)
                .endKafka()
                .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();

    private final CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.KUBERNETES_NAME_LABEL, CruiseControl.APPLICATION_NAME,
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
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS).withValue(CruiseControl.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_MIN_INSYNC_REPLICAS).withValue(minInsyncReplicas).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_DISK_MIB_CAPACITY).withValue(Double.toString(DEFAULT_BROKER_DISK_MIB_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY).withValue(Integer.toString(DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY).withValue(Double.toString(DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY).withValue(Double.toString(DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_CONFIGURATION).withValue(configuration.getConfiguration()).build());

        return expected;
    }

    public String getCapacityConfigurationFromEnvVar(Kafka resource, String envVar) {
        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        List<EnvVar> ccEnvVars = ccContainer.getEnv();

        return ccEnvVars.stream().filter(var -> envVar.equals(var.getName())).map(EnvVar::getValue).findFirst().get();
    }

    @Test
    public void testBrokerCapacities() {
        // Test user defined capacities
        BrokerCapacity userDefinedBrokerCapacity = new BrokerCapacity();
        userDefinedBrokerCapacity.setDisk("20000M");
        userDefinedBrokerCapacity.setCpuUtilization(95);
        userDefinedBrokerCapacity.setInboundNetwork("50000KB/s");
        userDefinedBrokerCapacity.setOutboundNetwork("50000KB/s");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .editKafka()
                    .withVersion(version)
                .endKafka()
                .withNewCruiseControl()
                    .withImage(ccImage)
                    .withBrokerCapacity(userDefinedBrokerCapacity)
                .endCruiseControl()
            .endSpec()
            .build();

        Capacity generatedCapacity = new Capacity(resource.getSpec());
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_DISK_MIB_CAPACITY), is(Double.toString(generatedCapacity.getDiskMiB())));
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY), is(Integer.toString(generatedCapacity.getCpuUtilization())));
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY), is(Double.toString(generatedCapacity.getInboundNetworkKiBPerSecond())));
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY), is(Double.toString(generatedCapacity.getOutboundNetworkKiBPerSecond())));

        // Test generated disk capacity
        JbodStorage jbodStorage = new JbodStorage();
        List<SingleVolumeStorage> volumes = new ArrayList<>();

        PersistentClaimStorage p1 = new PersistentClaimStorage();
        p1.setId(0);
        p1.setSize("50Gi");
        volumes.add(p1);

        PersistentClaimStorage p2 = new PersistentClaimStorage();
        p2.setId(1);
        p2.setSize("50G");
        volumes.add(p2);

        jbodStorage.setVolumes(volumes);

        resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .editKafka()
                    .withVersion(version)
                    .withStorage(jbodStorage)
                .endKafka()
                .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();

        generatedCapacity = new Capacity(resource.getSpec());
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_DISK_MIB_CAPACITY), is(Double.toString(generatedCapacity.getDiskMiB())));
    }

    @Test
    public void testFromConfigMap() {
        assertThat(cc.namespace, is(namespace));
        assertThat(cc.cluster, is(cluster));
        assertThat(cc.getImage(), is(ccImage));
    }

    @Test
    public void testGenerateDeployment() {
        Deployment dep = cc.generateDeployment(true, null, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(2));

        assertThat(dep.getMetadata().getName(), is(CruiseControlResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        assertThat(dep.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(dep.getMetadata().getOwnerReferences().get(0), is(cc.createOwnerReference()));

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(new Integer(CruiseControl.DEFAULT_HEALTHCHECK_DELAY)));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(new Integer(CruiseControl.DEFAULT_HEALTHCHECK_TIMEOUT)));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(new Integer(CruiseControl.DEFAULT_HEALTHCHECK_DELAY)));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(new Integer(CruiseControl.DEFAULT_HEALTHCHECK_TIMEOUT)));
        assertThat(ccContainer.getEnv(), is(getExpectedEnvVars()));
        assertThat(ccContainer.getPorts().size(), is(1));
        assertThat(ccContainer.getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(ccContainer.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(3));

        Volume volume = volumes.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControl.secretName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(AbstractModel.clusterCaCertSecretName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getConfigMap().getName(), is(CruiseControl.metricAndLogConfigsName(cluster)));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(3));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));
    }

    @Test
    public void testEnvVars()   {
        assertThat(cc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @Test
    public void testImagePullPolicy() {
        Deployment dep = cc.generateDeployment(true, null, ImagePullPolicy.ALWAYS, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = cc.generateDeployment(true, null,  ImagePullPolicy.IFNOTPRESENT, null);
        containers = dep.getSpec().getTemplate().getSpec().getContainers();
        ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @Test
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

        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, null, cruiseControlSpec);
        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        List<EnvVar> envVarList = cc.getEnvVars();

        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));
    }

    @Test
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

        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, null, cruiseControlSpec);
        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        List<EnvVar> envVarList = cc.getEnvVars();

        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));
    }

    @Test
    public void testCruiseControlNotDeployed() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, null,  null);
        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        try {
            assertThat(cc.generateDeployment(true, null, null, null), is(nullValue()));
            assertThat(cc.generateService(), is(nullValue()));
            assertThat(cc.generateSecret(null, true), is(nullValue()));
        } catch (Throwable expected) {
            assertEquals(NullPointerException.class, expected.getClass());
        }
    }

    @Test
    public void testGenerateService()   {
        Service svc = cc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(cc.getServiceName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(new Integer(CruiseControl.REST_API_PORT)));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        TestUtils.checkOwnerReference(cc.createOwnerReference(), svc);
    }

    @Test
    public void testGenerateServiceWhenDisabled()   {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, null, null);
        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        assertThrows(NullPointerException.class, () -> cc.generateService());
    }

    @Test
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withNewKey("key1")
                                    .withNewOperator("In")
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

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewCruiseControl()
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
                                .withNewPriorityClassName("top-priority")
                                .withNewSchedulerName("my-scheduler")
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                            .endPod()
                            .withNewApiService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnots)
                                .endMetadata()
                            .endApiService()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        // Check Deployment
        Deployment dep = cc.generateDeployment(true, depAnots, null, null);
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

        // Check Service
        svcLabels.putAll(expectedLabels());
        Service svc = cc.generateService();
        assertThat(svc.getMetadata().getLabels(), is(svcLabels));
        assertThat(svc.getMetadata().getAnnotations(),  is(svcAnots));
    }

    @Test
    public void testPodDisruptionBudget() {
        int maxUnavailable = 2;
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                        .withMaxUnavailable(maxUnavailable)
                    .endPodDisruptionBudget()
                .endTemplate()
                .build();

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .editKafka()
                        .withVersion(version)
                        .endKafka()
                        .withCruiseControl(cruiseControlSpec)
                        .endSpec()
                        .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true,  null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();

        PodDisruptionBudget pdb = cc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(maxUnavailable)));
    }

    @Test
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

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withVersion(version)
                    .endKafka()
                    .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();

        assertThat(ccContainer.getResources().getLimits(), is(limits));
        assertThat(ccContainer.getResources().getRequests(), is(requests));
    }

    @Test
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

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withVersion(version)
                    .endKafka()
                    .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(new Integer(healthDelay)));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(new Integer(healthTimeout)));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(new Integer(healthDelay)));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(new Integer(healthTimeout)));
    }

    @Test
    public void testSecurityContext() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig((Map) configuration.asOrderedProperties().asMap())
                .withNewTemplate()
                    .withNewPod()
                        .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                    .endPod()
                .endTemplate()
                .build();

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                            .withCruiseControl(cruiseControlSpec)
                        .endSpec()
                        .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        Deployment dep = cc.generateDeployment(true, null, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @Test
    public void testDefaultSecurityContext() {
        Deployment dep = cc.generateDeployment(true, null, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @Test
    public void testCruiseControlContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withNewReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig((Map) configuration.asOrderedProperties().asMap())
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withSecurityContext(securityContext)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                            .withCruiseControl(cruiseControlSpec)
                        .endSpec()
                        .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        Deployment dep = cc.generateDeployment(true, null, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(CruiseControl.CRUISE_CONTROL_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @Test
    public void testTlsSidecarContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withNewReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig((Map) configuration.asOrderedProperties().asMap())
                .withNewTemplate()
                    .withNewTlsSidecarContainer()
                        .withSecurityContext(securityContext)
                    .endTlsSidecarContainer()
                .endTemplate()
                .build();

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                            .withCruiseControl(cruiseControlSpec)
                        .endSpec()
                        .build();

        CruiseControl cc = CruiseControl.fromCrd(resource, VERSIONS);

        Deployment dep = cc.generateDeployment(true, null, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(CruiseControl.TLS_SIDECAR_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @Test
    public void testRestApiPortNetworkPolicy() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        NetworkPolicy np = cc.generateNetworkPolicy(true);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }


    @Test
    public void testGoalsCheck() {

        String customGoals = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal";

        Map<String, Object> customGoalConfig = (Map) configuration.asOrderedProperties().asMap();
        customGoalConfig.put(CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY.getName(), customGoals);

        CruiseControlSpec ccSpecWithCustomGoals = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(customGoalConfig)
                .build();

        Kafka resourceWithCustomGoals =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                                .withConfig(kafkaConfig)
                            .endKafka()
                            .withCruiseControl(ccSpecWithCustomGoals)
                        .endSpec()
                        .build();

        CruiseControl cruiseControlWithCustomGoals = CruiseControl.fromCrd(resourceWithCustomGoals, VERSIONS);

        String anomalyDetectionGoals =  cruiseControlWithCustomGoals
                .getConfiguration().asOrderedProperties().asMap()
                .get(CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY.getName());

        assertThat(anomalyDetectionGoals, is(customGoals));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
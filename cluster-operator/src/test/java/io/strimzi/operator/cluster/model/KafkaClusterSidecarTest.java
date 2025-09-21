/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.strimzi.api.kafka.model.common.SidecarContainer;
import io.strimzi.api.kafka.model.common.SidecarContainerBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity",
    "checkstyle:JavaNCSS"
})
public class KafkaClusterSidecarTest {
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final MockSharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "test";
    private static final String CLUSTER = "foo";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
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
                                    .build())
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    @Test
    public void testSidecarContainerNetworkPolicyGeneration() {
        SidecarContainer sidecarContainer1 = new SidecarContainerBuilder()
                .withName("metrics-sidecar")
                .withImage("metrics:latest")
                .withPorts(ContainerUtils.createContainerPort("metrics", 8080))
                .build();

        SidecarContainer sidecarContainer2 = new SidecarContainerBuilder()
                .withName("logging-sidecar") 
                .withImage("logging:latest")
                .withPorts(ContainerUtils.createContainerPort("logs", 8081))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .editOrNewTemplate()
                            .editOrNewPod()
                                .withSidecarContainers(List.of(sidecarContainer1, sidecarContainer2))
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        // Test basic network policy generation
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        // Verify sidecar container ports are included in network policy
        List<NetworkPolicyIngressRule> sidecarPort8080Rules = np.getSpec().getIngress().stream()
            .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(8080)))
            .collect(Collectors.toList());
        assertThat(sidecarPort8080Rules.size(), is(1));
        assertThat(sidecarPort8080Rules.get(0).getFrom(), is(empty())); // Open to all by default

        List<NetworkPolicyIngressRule> sidecarPort8081Rules = np.getSpec().getIngress().stream()
            .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(8081)))
            .collect(Collectors.toList());
        assertThat(sidecarPort8081Rules.size(), is(1));
        assertThat(sidecarPort8081Rules.get(0).getFrom(), is(empty())); // Open to all by default

        // Test duplicate ports scenario - create node pools with same sidecar port
        KafkaNodePool nodePool1 = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .editOrNewTemplate()
                        .editOrNewPod()
                            .withSidecarContainers(List.of(sidecarContainer1))
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaNodePool nodePool2 = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .editOrNewTemplate()
                        .editOrNewPod()
                            .withSidecarContainers(List.of(sidecarContainer1)) // Same sidecar with same port
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> duplicatePools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(nodePool1, nodePool2, POOL_MIXED), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster duplicateKc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, duplicatePools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        NetworkPolicy duplicateNp = duplicateKc.generateNetworkPolicy(null, null);

        // Verify only one rule is created for duplicate port across multiple node pools
        List<NetworkPolicyIngressRule> duplicatePortRules = duplicateNp.getSpec().getIngress().stream()
            .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(8080)))
            .collect(Collectors.toList());
        assertThat(duplicatePortRules.size(), is(1)); // Only one rule should exist for the duplicate port
    }

    @Test
    public void testSidecarContainerNetworkPolicyFromNodePools() {
        // Test 1: Create NodePool with sidecar containers
        SidecarContainer poolSidecar = new SidecarContainerBuilder()
                .withName("pool-sidecar")
                .withImage("pool-sidecar:latest")
                .withPorts(ContainerUtils.createContainerPort("pool-metrics", 8082))
                .build();

        KafkaNodePool nodePool = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .editOrNewTemplate()
                        .editOrNewPod()
                            .withSidecarContainers(List.of(poolSidecar))
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, nodePool), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        // Verify NodePool sidecar container ports are included in network policy
        List<NetworkPolicyIngressRule> poolSidecarRules = np.getSpec().getIngress().stream()
            .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(8082)))
            .collect(Collectors.toList());
        assertThat(poolSidecarRules.size(), is(1));
        assertThat(poolSidecarRules.get(0).getFrom(), is(empty())); // Open to all by default

        // Test 2: Verify network policy works without sidecar containers
        List<KafkaPool> basePools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster baseKc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, basePools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        NetworkPolicy baseNp = baseKc.generateNetworkPolicy(null, null);

        // Verify basic Kafka ports are present
        List<NetworkPolicyIngressRule> controlPlaneRules = baseNp.getSpec().getIngress().stream()
            .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT)))
            .collect(Collectors.toList());
        assertThat(controlPlaneRules.size(), is(1));

        List<NetworkPolicyIngressRule> replicationRules = baseNp.getSpec().getIngress().stream()
            .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT)))
            .collect(Collectors.toList());
        assertThat(replicationRules.size(), is(1));

        // Verify no sidecar ports are present in base case
        List<NetworkPolicyIngressRule> allRules = baseNp.getSpec().getIngress();
        Set<Integer> allPorts = allRules.stream()
            .map(rule -> rule.getPorts().get(0).getPort().getIntVal())
            .collect(Collectors.toSet());
        
        // Should not contain custom sidecar ports
        assertThat(allPorts.contains(8080), is(false));
        assertThat(allPorts.contains(8081), is(false));
        assertThat(allPorts.contains(8082), is(false));
    }

    @Test
    public void testSidecarContainersBasic() {
        SidecarContainer basicSidecar = new SidecarContainerBuilder()
                .withName("test-sidecar")
                .withImage("test-image:latest")
                .build();
        
        SidecarContainer envSidecar = new SidecarContainerBuilder()
                .withName("env-sidecar")
                .withImage("env-image:latest")
                .withEnv(List.of(
                    new ContainerEnvVarBuilder().withName("SIDECAR_MODE").withValue("production").build(),
                    new ContainerEnvVarBuilder().withName("LOG_LEVEL").withValue("debug").build()
                ))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .editOrNewTemplate()
                            .editOrNewPod()
                                .withSidecarContainers(List.of(basicSidecar, envSidecar))
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<Container> containers = pod.getSpec().getContainers();
            
            // Verify basic sidecar
            Optional<Container> basicOpt = containers.stream()
                    .filter(container -> "test-sidecar".equals(container.getName()))
                    .findFirst();
            assertThat(basicOpt.isPresent(), is(true));
            assertThat(basicOpt.get().getImage(), is("test-image:latest"));
            
            // Verify env sidecar
            Optional<Container> envOpt = containers.stream()
                    .filter(container -> "env-sidecar".equals(container.getName()))
                    .findFirst();
            assertThat(envOpt.isPresent(), is(true));
            Container envContainer = envOpt.get();
            assertThat(envContainer.getImage(), is("env-image:latest"));
            assertThat(envContainer.getEnv().size(), is(2));
            
            Optional<EnvVar> sidecarModeVar = envContainer.getEnv().stream()
                    .filter(env -> "SIDECAR_MODE".equals(env.getName())).findFirst();
            assertThat(sidecarModeVar.isPresent(), is(true));
            assertThat(sidecarModeVar.get().getValue(), is("production"));
            
            Optional<EnvVar> logLevelVar = envContainer.getEnv().stream()
                    .filter(env -> "LOG_LEVEL".equals(env.getName())).findFirst();
            assertThat(logLevelVar.isPresent(), is(true));
            assertThat(logLevelVar.get().getValue(), is("debug"));
        }));
    }

    @Test
    public void testSidecarContainersWithPorts() {
        SidecarContainer sidecarContainer = new SidecarContainerBuilder()
                .withName("metrics-sidecar")
                .withImage("metrics-image:v1.0")
                .withPorts(List.of(
                    ContainerUtils.createContainerPort("metrics", 8092),
                    ContainerUtils.createContainerPort("health", 8080)
                ))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .editOrNewTemplate()
                            .editOrNewPod()
                                .withSidecarContainers(List.of(sidecarContainer))
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Verify sidecar container with ports
            List<Container> containers = pod.getSpec().getContainers();
            Optional<Container> sidecarOpt = containers.stream()
                    .filter(container -> "metrics-sidecar".equals(container.getName()))
                    .findFirst();
            
            assertThat(sidecarOpt.isPresent(), is(true));
            Container actualSidecar = sidecarOpt.get();
            assertThat(actualSidecar.getImage(), is("metrics-image:v1.0"));
            assertThat(actualSidecar.getPorts().size(), is(2));
            assertThat(actualSidecar.getPorts().get(0).getName(), is("metrics"));
            assertThat(actualSidecar.getPorts().get(0).getContainerPort(), is(8092));
            assertThat(actualSidecar.getPorts().get(1).getName(), is("health"));
            assertThat(actualSidecar.getPorts().get(1).getContainerPort(), is(8080));
        }));
    }

    @Test
    public void testMultipleSidecarContainers() {
        List<SidecarContainer> sidecarContainers = List.of(
            new SidecarContainerBuilder()
                .withName("logging-sidecar")
                .withImage("logging-image:latest")
                .build(),
            new SidecarContainerBuilder()
                .withName("monitoring-sidecar")  
                .withImage("monitoring-image:latest")
                .withPorts(List.of(ContainerUtils.createContainerPort("metrics", 9095)))
                .build()
        );

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .editOrNewTemplate()
                            .editOrNewPod()
                                .withSidecarContainers(sidecarContainers)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Verify both sidecar containers are present
            List<Container> containers = pod.getSpec().getContainers();
            
            Optional<Container> loggingSidecarOpt = containers.stream()
                    .filter(container -> "logging-sidecar".equals(container.getName()))
                    .findFirst();
            assertThat(loggingSidecarOpt.isPresent(), is(true));
            assertThat(loggingSidecarOpt.get().getImage(), is("logging-image:latest"));
            
            Optional<Container> monitoringSidecarOpt = containers.stream()
                    .filter(container -> "monitoring-sidecar".equals(container.getName()))
                    .findFirst();
            assertThat(monitoringSidecarOpt.isPresent(), is(true));
            assertThat(monitoringSidecarOpt.get().getImage(), is("monitoring-image:latest"));
            assertThat(monitoringSidecarOpt.get().getPorts().size(), is(1));
            assertThat(monitoringSidecarOpt.get().getPorts().get(0).getName(), is("metrics"));
        }));
    }
}
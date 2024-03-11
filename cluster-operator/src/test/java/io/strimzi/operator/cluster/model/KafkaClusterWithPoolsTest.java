/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodeIdAssignment;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaClusterWithPoolsTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER_NAME = "my-cluster";
    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                        .endJbodStorage()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build())
                    .endKafka()
                .endSpec()
                .build();
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("Kafka")
            .withName(CLUSTER_NAME)
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private final static KafkaNodePool POOL_A = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-a")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
            .endSpec()
            .build();
    private final static KafkaPool KAFKA_POOL_A = KafkaPool.fromCrd(
            Reconciliation.DUMMY_RECONCILIATION,
            KAFKA,
            POOL_A,
            new NodeIdAssignment(Set.of(0, 1, 2), Set.of(0, 1, 2), Set.of(), Set.of(), Set.of()),
            null,
            OWNER_REFERENCE,
            SHARED_ENV_PROVIDER
    );
    private final static KafkaNodePool POOL_B = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-b")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("6"), "memory", new Quantity("20Gi"))).build())
            .endSpec()
            .build();
    private final static KafkaPool KAFKA_POOL_B = KafkaPool.fromCrd(
            Reconciliation.DUMMY_RECONCILIATION,
            KAFKA,
            POOL_B,
            new NodeIdAssignment(Set.of(10, 11), Set.of(10, 11), Set.of(), Set.of(), Set.of()),
            null,
            OWNER_REFERENCE,
            SHARED_ENV_PROVIDER
    );

    @Test
    public void testNodesAndStatuses()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        Set<NodeRef> nodes = kc.nodes();
        assertThat(nodes.size(), is(5));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER_NAME + "-pool-a-0", 0, "pool-a", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-a-1", 1, "pool-a", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-a-2", 2, "pool-a", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-b-10", 10, "pool-b", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-b-11", 11, "pool-b", false, true)));

        Set<NodeRef> brokerNodes = kc.brokerNodes();
        assertThat(brokerNodes.size(), is(5));
        assertThat(brokerNodes, hasItems(new NodeRef(CLUSTER_NAME + "-pool-a-0", 0, "pool-a", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-a-1", 1, "pool-a", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-a-2", 2, "pool-a", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-b-10", 10, "pool-b", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-b-11", 11, "pool-b", false, true)));

        Set<NodeRef> controllerNodes = kc.controllerNodes();
        assertThat(controllerNodes.size(), is(0)); // No KRaft cluster => 0 controller nodes

        Map<String, KafkaNodePoolStatus> statuses = kc.nodePoolStatuses();
        assertThat(statuses.size(), is(2));
        assertThat(statuses.get("pool-a").getReplicas(), is(3));
        assertThat(statuses.get("pool-a").getLabelSelector(), is("strimzi.io/cluster=my-cluster,strimzi.io/name=my-cluster-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=pool-a"));
        assertThat(statuses.get("pool-a").getNodeIds().size(), is(3));
        assertThat(statuses.get("pool-a").getNodeIds(), hasItems(0, 1, 2));
        assertThat(statuses.get("pool-a").getRoles().size(), is(1));
        assertThat(statuses.get("pool-a").getRoles(), hasItems(ProcessRoles.BROKER));
        assertThat(statuses.get("pool-b").getReplicas(), is(2));
        assertThat(statuses.get("pool-b").getLabelSelector(), is("strimzi.io/cluster=my-cluster,strimzi.io/name=my-cluster-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=pool-b"));
        assertThat(statuses.get("pool-b").getNodeIds().size(), is(2));
        assertThat(statuses.get("pool-b").getNodeIds(), hasItems(10, 11));
        assertThat(statuses.get("pool-b").getRoles().size(), is(1));
        assertThat(statuses.get("pool-b").getRoles(), hasItems(ProcessRoles.BROKER));
    }

    @Test
    public void testListenerResourcesWithInternalListenerOnly()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        assertThat(kc.generatePerPodServices().size(), is(0));
        assertThat(kc.generateExternalIngresses().size(), is(0));
        assertThat(kc.generateExternalRoutes().size(), is(0));
    }

    @Test
    public void testPerBrokerServices()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.CLUSTER_IP).withTls().build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-tls-0", "my-cluster-pool-a-tls-1", "my-cluster-pool-a-tls-2", "my-cluster-pool-b-tls-10", "my-cluster-pool-b-tls-11"));
    }

    @Test
    public void testPerBrokerServicesWithExternalListener()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.LOADBALANCER).withTls().build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-tls-0", "my-cluster-pool-a-tls-1", "my-cluster-pool-a-tls-2", "my-cluster-pool-b-tls-10", "my-cluster-pool-b-tls-11"));
    }

    @Test
    public void testIngresses()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9093)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("bootstrap.my-domain.tld")
                                    .endBootstrap()
                                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(0).withHost("broker-0.my-domain.tld").build(),
                                        new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1).withHost("broker-1.my-domain.tld").build(),
                                        new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(2).withHost("broker-2.my-domain.tld").build(),
                                        new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(10).withHost("broker-10.my-domain.tld").build(),
                                        new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(11).withHost("broker-11.my-domain.tld").build())
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-tls-0", "my-cluster-pool-a-tls-1", "my-cluster-pool-a-tls-2", "my-cluster-pool-b-tls-10", "my-cluster-pool-b-tls-11"));

        List<Ingress> ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(5));
        assertThat(ingresses.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-tls-0", "my-cluster-pool-a-tls-1", "my-cluster-pool-a-tls-2", "my-cluster-pool-b-tls-10", "my-cluster-pool-b-tls-11"));
    }

    @Test
    public void testRoutes()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.ROUTE).withTls().build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-tls-0", "my-cluster-pool-a-tls-1", "my-cluster-pool-a-tls-2", "my-cluster-pool-b-tls-10", "my-cluster-pool-b-tls-11"));

        List<Route> routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));
        assertThat(routes.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-tls-0", "my-cluster-pool-a-tls-1", "my-cluster-pool-a-tls-2", "my-cluster-pool-b-tls-10", "my-cluster-pool-b-tls-11"));
    }

    @Test
    public void testPodDisruptionBudgets()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(4));
    }

    @Test
    public void testPodSets()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        List<StrimziPodSet> podSets = kc.generatePodSets(false, null, null, i -> Map.of());
        assertThat(podSets.size(), is(2));
        assertThat(podSets.get(0).getMetadata().getName(), is("my-cluster-pool-a"));
        assertThat(podSets.get(0).getSpec().getPods().size(), is(3));
        assertThat(PodSetUtils.podNames(podSets.get(0)), hasItems("my-cluster-pool-a-0", "my-cluster-pool-a-1", "my-cluster-pool-a-2"));
        assertThat(PodSetUtils.podSetToPods(podSets.get(0)).get(0).getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));

        assertThat(podSets.get(1).getMetadata().getName(), is("my-cluster-pool-b"));
        assertThat(podSets.get(1).getSpec().getPods().size(), is(2));
        assertThat(PodSetUtils.podNames(podSets.get(1)), hasItems("my-cluster-pool-b-10", "my-cluster-pool-b-11"));
        assertThat(PodSetUtils.podSetToPods(podSets.get(1)).get(0).getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("6"), "memory", new Quantity("20Gi"))));
    }

    @Test
    public void testBrokerConfiguration()  {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("TLS_9093", "broker-0"),
                1, Map.of("TLS_9093", "broker-1"),
                2, Map.of("TLS_9093", "broker-2"),
                10, Map.of("TLS_9093", "broker-10"),
                11, Map.of("TLS_9093", "broker-11")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("TLS_9093", "9093"),
                1, Map.of("TLS_9093", "9093"),
                2, Map.of("TLS_9093", "9093"),
                10, Map.of("TLS_9093", "9093"),
                11, Map.of("TLS_9093", "9093")
        );

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                null, SHARED_ENV_PROVIDER
        );

        String configuration = kc.generatePerBrokerConfiguration(2, advertisedHostnames, advertisedPorts);
        assertThat(configuration, containsString("node.id=2\n"));
        assertThat(configuration, containsString("process.roles=broker\n"));

        configuration = kc.generatePerBrokerConfiguration(10, advertisedHostnames, advertisedPorts);
        assertThat(configuration, containsString("node.id=10\n"));
        assertThat(configuration, containsString("process.roles=broker\n"));

        List<ConfigMap> configMaps = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(null, null), advertisedHostnames, advertisedPorts);
        assertThat(configMaps.size(), is(5));
        assertThat(configMaps.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-pool-a-0", "my-cluster-pool-a-1", "my-cluster-pool-a-2", "my-cluster-pool-b-10", "my-cluster-pool-b-11"));

        ConfigMap broker2 = configMaps.stream().filter(cm -> "my-cluster-pool-a-2".equals(cm.getMetadata().getName())).findFirst().orElseThrow();
        assertThat(broker2.getData().get("server.config"), containsString("node.id=2\n"));
        assertThat(broker2.getData().get("server.config"), containsString("process.roles=broker\n"));

        ConfigMap broker10 = configMaps.stream().filter(cm -> "my-cluster-pool-b-10".equals(cm.getMetadata().getName())).findFirst().orElseThrow();
        assertThat(broker10.getData().get("server.config"), containsString("node.id=10\n"));
        assertThat(broker10.getData().get("server.config"), containsString("process.roles=broker\n"));
    }

    @Test
    public void testStorageAndResourcesForCruiseControl()   {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_A, KAFKA_POOL_B),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                null, SHARED_ENV_PROVIDER
        );

        Map<String, Storage> storage = kc.getStorageByPoolName();
        assertThat(storage.size(), is(2));
        assertThat(storage.get("pool-a"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(storage.get("pool-b"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build()).build()));

        Map<String, ResourceRequirements> resources = kc.getBrokerResourceRequirementsByPoolName();
        assertThat(resources.size(), is(2));
        assertThat(resources.get("pool-a").getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));
        assertThat(resources.get("pool-b").getRequests(), is(Map.of("cpu", new Quantity("6"), "memory", new Quantity("20Gi"))));
    }

    @Test
    public void testCruiseControlWithSingleKafkaBroker() {
        Map<String, Object> config = new HashMap<>();
        config.put("offsets.topic.replication.factor", 1);
        config.put("transaction.state.log.replication.factor", 1);
        config.put("transaction.state.log.min.isr", 1);

        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(1)
                .endSpec()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withReplicas(1)
                        .withConfig(config)
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        // Test exception being raised when only one broker is present
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(poolA), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), is("Kafka " + NAMESPACE + "/" + CLUSTER_NAME + " has invalid configuration. " +
                "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers."));

        // Test if works fine with 2 brokers in 2 different pools
        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withReplicas(1)
                .endSpec()
                .build();

        assertDoesNotThrow(() -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(poolA, poolB), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }
}

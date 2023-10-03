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
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
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

public class KafkaClusterWithKRaftTest {
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
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
            .endSpec()
            .build();
    private final static KafkaPool KAFKA_POOL_CONTROLLERS = KafkaPool.fromCrd(
            Reconciliation.DUMMY_RECONCILIATION,
            KAFKA,
            POOL_CONTROLLERS,
            new NodeIdAssignment(Set.of(0, 1, 2), Set.of(0, 1, 2), Set.of(), Set.of()),
            null,
            OWNER_REFERENCE,
            SHARED_ENV_PROVIDER
    );
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
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
            .endSpec()
            .build();
    private final static KafkaPool KAFKA_POOL_BROKERS = KafkaPool.fromCrd(
            Reconciliation.DUMMY_RECONCILIATION,
            KAFKA,
            POOL_BROKERS,
            new NodeIdAssignment(Set.of(1000, 1001, 1002), Set.of(1000, 1001, 1002), Set.of(), Set.of()),
            null,
            OWNER_REFERENCE,
            SHARED_ENV_PROVIDER
    );

    @Test
    public void testNodesAndStatuses()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null, SHARED_ENV_PROVIDER
        );

        Set<NodeRef> nodes = kc.nodes();
        assertThat(nodes.size(), is(6));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER_NAME + "-controllers-0", 0, "controllers", true, false),
                new NodeRef(CLUSTER_NAME + "-controllers-1", 1, "controllers", true, false),
                new NodeRef(CLUSTER_NAME + "-controllers-2", 2, "controllers", true, false),
                new NodeRef(CLUSTER_NAME + "-brokers-1000", 1000, "brokers", false, true),
                new NodeRef(CLUSTER_NAME + "-brokers-1001", 1001, "brokers", false, true),
                new NodeRef(CLUSTER_NAME + "-brokers-1002", 1002, "brokers", false, true)));

        Set<NodeRef> brokerNodes = kc.brokerNodes();
        assertThat(brokerNodes.size(), is(3));
        assertThat(brokerNodes, hasItems(new NodeRef(CLUSTER_NAME + "-brokers-1000", 1000, "brokers", false, true),
                new NodeRef(CLUSTER_NAME + "-brokers-1001", 1001, "brokers", false, true),
                new NodeRef(CLUSTER_NAME + "-brokers-1002", 1002, "brokers", false, true)));

        Set<NodeRef> controllerNodes = kc.controllerNodes();
        assertThat(controllerNodes.size(), is(3));
        assertThat(controllerNodes, hasItems(new NodeRef(CLUSTER_NAME + "-controllers-0", 0, "controllers", true, false),
                new NodeRef(CLUSTER_NAME + "-controllers-1", 1, "controllers", true, false),
                new NodeRef(CLUSTER_NAME + "-controllers-2", 2, "controllers", true, false)));

        Map<String, KafkaNodePoolStatus> statuses = kc.nodePoolStatuses();
        assertThat(statuses.size(), is(2));
        assertThat(statuses.get("controllers").getReplicas(), is(3));
        assertThat(statuses.get("controllers").getLabelSelector(), is("strimzi.io/cluster=my-cluster,strimzi.io/name=my-cluster-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=controllers"));
        assertThat(statuses.get("controllers").getNodeIds().size(), is(3));
        assertThat(statuses.get("controllers").getNodeIds(), hasItems(0, 1, 2));
        assertThat(statuses.get("brokers").getReplicas(), is(3));
        assertThat(statuses.get("brokers").getLabelSelector(), is("strimzi.io/cluster=my-cluster,strimzi.io/name=my-cluster-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=brokers"));
        assertThat(statuses.get("brokers").getNodeIds().size(), is(3));
        assertThat(statuses.get("brokers").getNodeIds(), hasItems(1000, 1001, 1002));
    }

    @Test
    public void testListenerResourcesWithInternalListenerOnly()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null,
                SHARED_ENV_PROVIDER
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
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null,
                SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(3));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-brokers-tls-1000", "my-cluster-brokers-tls-1001", "my-cluster-brokers-tls-1002"));
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
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null,
                SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(3));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-brokers-tls-1000", "my-cluster-brokers-tls-1001", "my-cluster-brokers-tls-1002"));
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
                                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1000).withHost("broker-1000.my-domain.tld").build(),
                                        new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1001).withHost("broker-1001.my-domain.tld").build(),
                                        new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1002).withHost("broker-1002.my-domain.tld").build())
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null,
                SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(3));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-brokers-tls-1000", "my-cluster-brokers-tls-1001", "my-cluster-brokers-tls-1002"));

        List<Ingress> ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(3));
        assertThat(ingresses.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-brokers-tls-1000", "my-cluster-brokers-tls-1001", "my-cluster-brokers-tls-1002"));
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
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null,
                SHARED_ENV_PROVIDER
        );

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(3));
        assertThat(services.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-brokers-tls-1000", "my-cluster-brokers-tls-1001", "my-cluster-brokers-tls-1002"));

        List<Route> routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(3));
        assertThat(routes.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-brokers-tls-1000", "my-cluster-brokers-tls-1001", "my-cluster-brokers-tls-1002"));
    }

    @Test
    public void testBrokerConfiguration()  {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                1000, Map.of("TLS_9093", "broker-1000"),
                1001, Map.of("TLS_9093", "broker-1001"),
                1002, Map.of("TLS_9093", "broker-1002")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                1000, Map.of("TLS_9093", "9093"),
                1001, Map.of("TLS_9093", "9093"),
                1002, Map.of("TLS_9093", "9093")
        );

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null,
                SHARED_ENV_PROVIDER
        );

        String configuration = kc.generatePerBrokerConfiguration(2, Map.of(), Map.of());
        assertThat(configuration, containsString("node.id=2\n"));
        assertThat(configuration, containsString("process.roles=controller\n"));

        configuration = kc.generatePerBrokerConfiguration(1001, advertisedHostnames, advertisedPorts);
        assertThat(configuration, containsString("node.id=1001\n"));
        assertThat(configuration, containsString("process.roles=broker\n"));

        List<ConfigMap> configMaps = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(null, null), advertisedHostnames, advertisedPorts);
        assertThat(configMaps.size(), is(6));
        assertThat(configMaps.stream().map(s -> s.getMetadata().getName()).toList(), hasItems("my-cluster-controllers-0", "my-cluster-controllers-1", "my-cluster-controllers-2", "my-cluster-brokers-1000", "my-cluster-brokers-1001", "my-cluster-brokers-1002"));

        ConfigMap broker2 = configMaps.stream().filter(cm -> "my-cluster-controllers-2".equals(cm.getMetadata().getName())).findFirst().orElseThrow();
        assertThat(broker2.getData().get("server.config"), containsString("node.id=2\n"));
        assertThat(broker2.getData().get("server.config"), containsString("process.roles=controller\n"));

        ConfigMap broker10 = configMaps.stream().filter(cm -> "my-cluster-brokers-1001".equals(cm.getMetadata().getName())).findFirst().orElseThrow();
        assertThat(broker10.getData().get("server.config"), containsString("node.id=1001\n"));
        assertThat(broker10.getData().get("server.config"), containsString("process.roles=broker\n"));
    }

    @Test
    public void testStorageAndResourcesForCruiseControl()   {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                true,
                null, SHARED_ENV_PROVIDER
        );

        Map<String, Storage> storage = kc.getStorageByPoolName();
        assertThat(storage.size(), is(2));
        assertThat(storage.get("controllers"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(storage.get("brokers"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));

        Map<String, ResourceRequirements> resources = kc.getBrokerResourceRequirementsByPoolName();
        assertThat(resources.size(), is(1));
        assertThat(resources.get("brokers").getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));
    }

    @Test
    public void testCruiseControlWithSingleKafkaBroker() {
        Map<String, Object> config = new HashMap<>();
        config.put("offsets.topic.replication.factor", 1);
        config.put("transaction.state.log.replication.factor", 1);
        config.put("transaction.state.log.min.isr", 1);

        KafkaNodePool poolBrokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER)
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
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, poolBrokers), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, true, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), is("Kafka " + NAMESPACE + "/" + CLUSTER_NAME + " has invalid configuration. " +
                "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers."));

        // Test if works fine with controller pool and broker pool with more than 1 node
        assertDoesNotThrow(() -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_BROKERS), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, true, null, SHARED_ENV_PROVIDER);
        });
    }
}

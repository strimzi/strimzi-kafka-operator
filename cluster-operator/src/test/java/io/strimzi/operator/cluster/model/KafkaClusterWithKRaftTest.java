/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
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
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.nodepools.NodeIdAssignment;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
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
            new NodeIdAssignment(Set.of(0, 1, 2), Set.of(0, 1, 2), Set.of(), Set.of(), Set.of()),
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
            new NodeIdAssignment(Set.of(1000, 1001, 1002), Set.of(1000, 1001, 1002), Set.of(), Set.of(), Set.of()),
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
        assertThat(statuses.get("controllers").getRoles().size(), is(1));
        assertThat(statuses.get("controllers").getRoles(), hasItems(ProcessRoles.CONTROLLER));
        assertThat(statuses.get("brokers").getReplicas(), is(3));
        assertThat(statuses.get("brokers").getLabelSelector(), is("strimzi.io/cluster=my-cluster,strimzi.io/name=my-cluster-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=brokers"));
        assertThat(statuses.get("brokers").getNodeIds().size(), is(3));
        assertThat(statuses.get("brokers").getNodeIds(), hasItems(1000, 1001, 1002));
        assertThat(statuses.get("brokers").getRoles().size(), is(1));
        assertThat(statuses.get("brokers").getRoles(), hasItems(ProcessRoles.BROKER));
    }

    @Test
    public void testListenerResourcesWithInternalListenerOnly()  {
        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                "my-cluster-id",
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

        configMaps.forEach(cm -> assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("my-cluster-id")));

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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
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
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), is("Kafka " + NAMESPACE + "/" + CLUSTER_NAME + " has invalid configuration. " +
                "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers."));

        // Test if works fine with controller pool and broker pool with more than 1 node
        assertDoesNotThrow(() -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_BROKERS), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        });
    }

    @Test
    public void testGenerateBrokerSecretExternalWithManyDNS() throws CertificateParsingException {
        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), CLUSTER_NAME, null, null);
        clusterCa.createRenewOrReplace(NAMESPACE, CLUSTER_NAME, emptyMap(), emptyMap(), emptyMap(), null, true);
        ClientsCa clientsCa = new ClientsCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), null, null, null, null, 365, 30, true, CertificateExpirationPolicy.RENEW_CERTIFICATE);
        clientsCa.createRenewOrReplace(NAMESPACE, CLUSTER_NAME, emptyMap(), emptyMap(), emptyMap(), null, true);

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                null, SHARED_ENV_PROVIDER
        );

        Map<Integer, Set<String>> externalAddresses = new HashMap<>();
        externalAddresses.put(1000, TestUtils.set("123.10.125.130", "my-broker-1000"));
        externalAddresses.put(1001, TestUtils.set("123.10.125.131", "my-broker-1001"));
        externalAddresses.put(1002, TestUtils.set("123.10.125.132", "my-broker-1002"));

        Secret secret = kc.generateCertificatesSecret(clusterCa, clientsCa, TestUtils.set("123.10.125.140", "my-bootstrap"), externalAddresses, true);
        assertThat(secret.getData().keySet(), is(set(
                "my-cluster-controllers-0.crt",  "my-cluster-controllers-0.key", "my-cluster-controllers-0.p12", "my-cluster-controllers-0.password",
                "my-cluster-controllers-1.crt", "my-cluster-controllers-1.key", "my-cluster-controllers-1.p12", "my-cluster-controllers-1.password",
                "my-cluster-controllers-2.crt", "my-cluster-controllers-2.key", "my-cluster-controllers-2.p12", "my-cluster-controllers-2.password",
                "my-cluster-brokers-1000.crt",  "my-cluster-brokers-1000.key", "my-cluster-brokers-1000.p12", "my-cluster-brokers-1000.password",
                "my-cluster-brokers-1001.crt", "my-cluster-brokers-1001.key", "my-cluster-brokers-1001.p12", "my-cluster-brokers-1001.password",
                "my-cluster-brokers-1002.crt", "my-cluster-brokers-1002.key", "my-cluster-brokers-1002.p12", "my-cluster-brokers-1002.password")));

        // Controller cert
        X509Certificate cert = Ca.cert(secret, "my-cluster-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=my-cluster-kafka,O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local"),
                asList(2, "my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc"),
                asList(2, "my-cluster-kafka-bootstrap"),
                asList(2, "my-cluster-kafka-bootstrap.my-namespace"),
                asList(2, "my-cluster-kafka-bootstrap.my-namespace.svc"),
                asList(2, "my-cluster-kafka-bootstrap.my-namespace.svc.cluster.local"),
                asList(2, "my-cluster-kafka-brokers"),
                asList(2, "my-cluster-kafka-brokers.my-namespace"),
                asList(2, "my-cluster-kafka-brokers.my-namespace.svc"),
                asList(2, "my-cluster-kafka-brokers.my-namespace.svc.cluster.local"))));

        // Broker cert
        cert = Ca.cert(secret, "my-cluster-brokers-1000.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=my-cluster-kafka,O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "my-cluster-brokers-1000.my-cluster-kafka-brokers.my-namespace.svc.cluster.local"),
                asList(2, "my-cluster-brokers-1000.my-cluster-kafka-brokers.my-namespace.svc"),
                asList(2, "my-cluster-kafka-bootstrap"),
                asList(2, "my-cluster-kafka-bootstrap.my-namespace"),
                asList(2, "my-cluster-kafka-bootstrap.my-namespace.svc"),
                asList(2, "my-cluster-kafka-bootstrap.my-namespace.svc.cluster.local"),
                asList(2, "my-cluster-kafka-brokers"),
                asList(2, "my-cluster-kafka-brokers.my-namespace"),
                asList(2, "my-cluster-kafka-brokers.my-namespace.svc"),
                asList(2, "my-cluster-kafka-brokers.my-namespace.svc.cluster.local"),
                asList(2, "my-broker-1000"),
                asList(2, "my-bootstrap"),
                asList(7, "123.10.125.140"),
                asList(7, "123.10.125.130"))));
    }

    @Test
    public void testExternalAddressEnvVarNotSetInControllers() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewRack()
                            .withTopologyKey("my-topology-key")
                        .endRack()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                null,
                SHARED_ENV_PROVIDER
        );

        // Controller
        List<EnvVar> envVars = kc.getInitContainerEnvVars(KAFKA_POOL_CONTROLLERS);
        assertThat(envVars.size(), is(2));
        assertThat(envVars.get(0).getName(), is("NODE_NAME"));
        assertThat(envVars.get(0).getValueFrom(), is(notNullValue()));
        assertThat(envVars.get(1).getName(), is("RACK_TOPOLOGY_KEY"));
        assertThat(envVars.get(1).getValue(), is("my-topology-key"));

        // Brokers
        envVars = kc.getInitContainerEnvVars(KAFKA_POOL_BROKERS);
        assertThat(envVars.size(), is(3));
        assertThat(envVars.get(0).getName(), is("NODE_NAME"));
        assertThat(envVars.get(0).getValueFrom(), is(notNullValue()));
        assertThat(envVars.get(1).getName(), is("RACK_TOPOLOGY_KEY"));
        assertThat(envVars.get(1).getValue(), is("my-topology-key"));
        assertThat(envVars.get(2).getName(), is("EXTERNAL_ADDRESS"));
        assertThat(envVars.get(2).getValue(), is("TRUE"));
    }

    @Test
    public void testConfigMapFields() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                1000, Map.of("PLAIN_9092", "broker-0"),
                1001, Map.of("PLAIN_9092", "broker-1"),
                1002, Map.of("PLAIN_9092", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                1000, Map.of("PLAIN_9092", "9092"),
                1001, Map.of("PLAIN_9092", "9092"),
                1002, Map.of("PLAIN_9092", "9092")
        );

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                "dummy-cluster-id",
                SHARED_ENV_PROVIDER
        );

        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(null, null), advertisedHostnames, advertisedPorts);

        assertThat(cms.size(), is(6));

        for (ConfigMap cm : cms)    {
            if (cm.getMetadata().getName().contains("-controllers-"))   {
                // Controllers
                assertThat(cm.getData().size(), is(6));
                assertThat(cm.getData().get(LoggingModel.LOG4J1_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_METADATA_VERSION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_LISTENERS_FILENAME), is(nullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_METADATA_STATE_FILENAME), is(notNullValue()));
            } else {
                // Brokers
                assertThat(cm.getData().size(), is(6));
                assertThat(cm.getData().get(LoggingModel.LOG4J1_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_METADATA_VERSION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_LISTENERS_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_METADATA_STATE_FILENAME), is(notNullValue()));
            }
        }
    }

    @Test
    public void testContainerPorts() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build(),
                                new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("metrics-cm", "metrics.json", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                null,
                SHARED_ENV_PROVIDER
        );

        // Controller
        List<ContainerPort> ports = kc.getContainerPortList(KAFKA_POOL_CONTROLLERS);
        assertThat(ports.size(), is(2));
        assertThat(ports.get(0).getContainerPort(), is(9090));
        assertThat(ports.get(1).getContainerPort(), is(9404));

        // Brokers
        ports = kc.getContainerPortList(KAFKA_POOL_BROKERS);
        assertThat(ports.size(), is(4));
        assertThat(ports.get(0).getContainerPort(), is(9091));
        assertThat(ports.get(1).getContainerPort(), is(9093));
        assertThat(ports.get(2).getContainerPort(), is(9094));
        assertThat(ports.get(3).getContainerPort(), is(9404));
    }

    @Test
    public void testKRaftMetadataVersionValidation()   {
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
                        List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                        VERSIONS,
                        new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, "3.6-IV9"),
                        KafkaMetadataConfigurationState.KRAFT,
                        null,
                        SHARED_ENV_PROVIDER));
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
                List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                VERSIONS,
                new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, "3.5-IV1"),
                KafkaMetadataConfigurationState.KRAFT,
                null,
                SHARED_ENV_PROVIDER);

        assertThat(kc.getMetadataVersion(), is("3.5-IV1"));
    }
}

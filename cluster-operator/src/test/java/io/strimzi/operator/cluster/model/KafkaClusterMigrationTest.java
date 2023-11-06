/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodeIdAssignment;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaClusterMigrationTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER = "my-cluster";
    private final static int REPLICAS = 3;

    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder().withName("plain").withPort(9092).withType(KafkaListenerType.INTERNAL).build())
                .endKafka()
            .endSpec()
            .build();

    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("Kafka")
            .withName(CLUSTER)
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();

    private final static KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(REPLICAS)
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaPool KAFKA_POOL_BROKERS = KafkaPool.fromCrd(
            Reconciliation.DUMMY_RECONCILIATION,
            KAFKA,
            POOL_BROKERS,
            new NodeIdAssignment(Set.of(0, 1, 2), Set.of(0, 1, 2), Set.of(), Set.of(), Set.of()),
            null,
            OWNER_REFERENCE,
            SHARED_ENV_PROVIDER
    );
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(REPLICAS)
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaPool KAFKA_POOL_CONTROLLERS = KafkaPool.fromCrd(
            Reconciliation.DUMMY_RECONCILIATION,
            KAFKA,
            POOL_CONTROLLERS,
            new NodeIdAssignment(Set.of(3, 4, 5), Set.of(3, 4, 5), Set.of(), Set.of(), Set.of()),
            null,
            OWNER_REFERENCE,
            SHARED_ENV_PROVIDER
    );

    private final static KafkaVersionChange KAFKA_VERSION_CHANGE = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

    @Test
    public void testBrokerNodeConfigurationOnMigration() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc"),
                1, Map.of("PLAIN_9092", "my-cluster-brokers-1.my-cluster-kafka-brokers.my-namespace.svc"),
                2, Map.of("PLAIN_9092", "my-cluster-brokers-2.my-cluster-kafka-brokers.my-namespace.svc")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092"),
                1, Map.of("PLAIN_9092", "9092"),
                2, Map.of("PLAIN_9092", "9092")
        );

        for (KafkaMetadataConfigurationState state : KafkaMetadataConfigurationState.values()) {
            KafkaCluster kc = KafkaCluster.fromCrd(
                    Reconciliation.DUMMY_RECONCILIATION,
                    KAFKA,
                    List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                    VERSIONS,
                    KAFKA_VERSION_CHANGE,
                    state,
                    null, SHARED_ENV_PROVIDER);

            String configuration = kc.generatePerBrokerConfiguration(0, advertisedHostnames, advertisedPorts);

            assertThat(configuration, containsString("node.id=0"));
            // from ZK up to MIGRATION ...
            if (state.isZooKeeperToMigration()) {
                // ... has ZooKeeper connection configured
                assertThat(configuration, containsString("zookeeper.connect"));
                // ... broker.id still set
                assertThat(configuration, containsString("broker.id=0"));
                // ... control plane is set as listener and advertised
                assertThat(configuration, containsString("control.plane.listener.name=CONTROLPLANE-9090"));
                assertThat(configuration, containsString("listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092"));
                assertThat(configuration, containsString("advertised.listeners=CONTROLPLANE-9090://my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc:9092"));
            } else {
                assertThat(configuration, not(containsString("zookeeper.connect")));
                assertThat(configuration, not(containsString("broker.id=0")));
                assertThat(configuration, not(containsString("control.plane.listener.name=CONTROLPLANE-9090")));
                assertThat(configuration, containsString("listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092"));
                assertThat(configuration, containsString("advertised.listeners=REPLICATION-9091://my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc:9092"));
            }
            // only during MIGRATION, the broker has the ZooKeeper migration flag enabled
            if (state.isMigration()) {
                assertThat(configuration, containsString("zookeeper.metadata.migration.enable=true"));
            } else {
                assertThat(configuration, not(containsString("zookeeper.metadata.migration.enable")));
            }
            // from MIGRATION up to KRAFT, the broker has KRaft controllers configured
            if (state.isMigrationToKRaft()) {
                assertThat(configuration, containsString("controller.listener.names=CONTROLPLANE-9090"));
                assertThat(configuration, containsString("controller.quorum.voters=3@my-cluster-controllers-3.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,4@my-cluster-controllers-4.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,5@my-cluster-controllers-5.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090"));
            } else {
                assertThat(configuration, not(containsString("controller.listener.names")));
                assertThat(configuration, not(containsString("controller.quorum.voters")));
            }
            // only from POST_MIGRATION to KRAFT, the broker has the process role configured
            if (state.isPostMigrationToKRaft()) {
                assertThat(configuration, containsString("process.roles=broker"));
            } else {
                assertThat(configuration, not(containsString("process.roles=broker")));
            }
            assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT"));
            assertThat(configuration, containsString("inter.broker.listener.name=REPLICATION-9091"));
        }
    }

    @Test
    public void testControllerNodeConfigurationOnMigration() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                3, Map.of("PLAIN_9092", "my-cluster-controllers-3.my-cluster-kafka-brokers.my-namespace.svc"),
                4, Map.of("PLAIN_9092", "my-cluster-controllers-4.my-cluster-kafka-brokers.my-namespace.svc"),
                5, Map.of("PLAIN_9092", "my-cluster-controllers-5.my-cluster-kafka-brokers.my-namespace.svc")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                3, Map.of("PLAIN_9092", "9092"),
                4, Map.of("PLAIN_9092", "9092"),
                5, Map.of("PLAIN_9092", "9092")
        );

        for (KafkaMetadataConfigurationState state : KafkaMetadataConfigurationState.values()) {
            // controllers don't make sense in ZooKeeper state, but only from pre-migration to KRaft
            if (state.isPreMigrationToKRaft()) {
                KafkaCluster kc = KafkaCluster.fromCrd(
                        Reconciliation.DUMMY_RECONCILIATION,
                        KAFKA,
                        List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                        VERSIONS,
                        KAFKA_VERSION_CHANGE,
                        state,
                        null, SHARED_ENV_PROVIDER);

                String configuration = kc.generatePerBrokerConfiguration(3, advertisedHostnames, advertisedPorts);

                // controllers always have node.id and process role set
                assertThat(configuration, containsString("node.id=3"));
                assertThat(configuration, containsString("process.roles=controller"));
                // controllers don't have broker.id at all, only node.id
                assertThat(configuration, not(containsString("broker.id")));

                // from PRE_MIGRATION up to POST_MIGRATION ...
                if (state.isPreMigrationToKRaft() && !state.isKRaft()) {
                    // ... has ZooKeeper connection configured
                    assertThat(configuration, containsString("zookeeper.connect"));
                    // ... has the ZooKeeper migration flag enabled
                    assertThat(configuration, containsString("zookeeper.metadata.migration.enable=true"));
                } else {
                    assertThat(configuration, not(containsString("zookeeper.connect")));
                    assertThat(configuration, not(containsString("zookeeper.metadata.migration.enable")));
                }

                // up to POST_MIGRATION ...
                if (state.isZooKeeperToPostMigration()) {
                    // .. replication listener configured, before being full KRaft
                    assertThat(configuration, containsString("listener.name.replication-9091"));
                    assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL"));
                    assertThat(configuration, containsString("inter.broker.listener.name=REPLICATION-9091"));
                } else {
                    assertThat(configuration, not(containsString("listener.name.replication-9091")));
                    assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL"));
                    assertThat(configuration, not(containsString("inter.broker.listener.name=REPLICATION-9091")));
                }

                assertThat(configuration, containsString("listener.name.controlplane-9090"));
                assertThat(configuration, containsString("listeners=CONTROLPLANE-9090://0.0.0.0:9090"));
                // controllers never advertises listeners
                assertThat(configuration, not(containsString("advertised.listeners")));

                assertThat(configuration, containsString("controller.listener.names=CONTROLPLANE-9090"));
                assertThat(configuration, containsString("controller.quorum.voters=3@my-cluster-controllers-3.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,4@my-cluster-controllers-4.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,5@my-cluster-controllers-5.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090"));
            }
        }
    }

    @Test
    public void testPortsOnMigration() {
        for (KafkaMetadataConfigurationState state : KafkaMetadataConfigurationState.values()) {
            KafkaCluster kc = KafkaCluster.fromCrd(
                    Reconciliation.DUMMY_RECONCILIATION,
                    KAFKA,
                    List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                    VERSIONS,
                    KAFKA_VERSION_CHANGE,
                    state,
                    null,
                    SHARED_ENV_PROVIDER
            );

            // controllers
            List<ContainerPort> ports = kc.getContainerPortList(KAFKA_POOL_CONTROLLERS);
            // control plane port is always set
            assertThat(ports.get(0).getContainerPort(), is(9090));
            if (state.isZooKeeperToPostMigration()) {
                assertThat(ports.size(), is(3));
                // replication and clients only up to post-migration to contact brokers
                assertThat(ports.get(1).getContainerPort(), is(9091));
                assertThat(ports.get(2).getContainerPort(), is(9092));
            } else {
                assertThat(ports.size(), is(1));
            }

            // brokers
            ports = kc.getContainerPortList(KAFKA_POOL_BROKERS);
            if (state.isZooKeeperToMigration()) {
                assertThat(ports.size(), is(3));
                // control plane port exposed up to migration when it's still ZooKeeper in the configuration
                assertThat(ports.get(0).getContainerPort(), is(9090));
                assertThat(ports.get(1).getContainerPort(), is(9091));
                assertThat(ports.get(2).getContainerPort(), is(9092));
            } else {
                assertThat(ports.size(), is(2));
                assertThat(ports.get(0).getContainerPort(), is(9091));
                assertThat(ports.get(1).getContainerPort(), is(9092));
            }
        }
    }

    @Test
    public void testConfigurationConfigMapsOnMigration() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc"),
                1, Map.of("PLAIN_9092", "my-cluster-brokers-1.my-cluster-kafka-brokers.my-namespace.svc"),
                2, Map.of("PLAIN_9092", "my-cluster-brokers-2.my-cluster-kafka-brokers.my-namespace.svc")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092"),
                1, Map.of("PLAIN_9092", "9092"),
                2, Map.of("PLAIN_9092", "9092")
        );

        for (KafkaMetadataConfigurationState state : KafkaMetadataConfigurationState.values()) {
            KafkaCluster kc = KafkaCluster.fromCrd(
                    Reconciliation.DUMMY_RECONCILIATION,
                    KAFKA,
                    List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                    VERSIONS,
                    KAFKA_VERSION_CHANGE,
                    state,
                    null,
                    SHARED_ENV_PROVIDER
            );

            List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(null, null), advertisedHostnames, advertisedPorts);
            for (ConfigMap cm : cms)    {
                assertThat(cm.getData().get("metadata.state"), is(String.valueOf(state.ordinal())));
            }
        }
    }

    @Test
    public void testBrokerNodeAuthorizerOnMigration() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withAuthorization(new KafkaAuthorizationSimple())
                    .endKafka()
                .endSpec()
                .build();

        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "my-cluster-brokers-0.my-cluster-kafka-brokers.my-namespace.svc"),
                1, Map.of("PLAIN_9092", "my-cluster-brokers-1.my-cluster-kafka-brokers.my-namespace.svc"),
                2, Map.of("PLAIN_9092", "my-cluster-brokers-2.my-cluster-kafka-brokers.my-namespace.svc")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092"),
                1, Map.of("PLAIN_9092", "9092"),
                2, Map.of("PLAIN_9092", "9092")
        );

        for (KafkaMetadataConfigurationState state : KafkaMetadataConfigurationState.values()) {
            KafkaCluster kc = KafkaCluster.fromCrd(
                    Reconciliation.DUMMY_RECONCILIATION,
                    kafka,
                    List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                    VERSIONS,
                    KAFKA_VERSION_CHANGE,
                    state,
                    null, SHARED_ENV_PROVIDER);

            String configuration = kc.generatePerBrokerConfiguration(0, advertisedHostnames, advertisedPorts);

            if (state.isPostMigrationToKRaft()) {
                assertThat(configuration, containsString("authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"));
            } else {
                assertThat(configuration, containsString("authorizer.class.name=kafka.security.authorizer.AclAuthorizer"));
            }
        }
    }

    @Test
    public void testControllerNodeAuthorizerOnMigration() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withAuthorization(new KafkaAuthorizationSimple())
                    .endKafka()
                .endSpec()
                .build();

        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                3, Map.of("PLAIN_9092", "my-cluster-controllers-3.my-cluster-kafka-brokers.my-namespace.svc"),
                4, Map.of("PLAIN_9092", "my-cluster-controllers-4.my-cluster-kafka-brokers.my-namespace.svc"),
                5, Map.of("PLAIN_9092", "my-cluster-controllers-5.my-cluster-kafka-brokers.my-namespace.svc")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                3, Map.of("PLAIN_9092", "9092"),
                4, Map.of("PLAIN_9092", "9092"),
                5, Map.of("PLAIN_9092", "9092")
        );

        for (KafkaMetadataConfigurationState state : KafkaMetadataConfigurationState.values()) {
            // controllers don't make sense in ZooKeeper state, but only from pre-migration to KRaft
            if (state.isPreMigrationToKRaft()) {
                KafkaCluster kc = KafkaCluster.fromCrd(
                        Reconciliation.DUMMY_RECONCILIATION,
                        kafka,
                        List.of(KAFKA_POOL_CONTROLLERS, KAFKA_POOL_BROKERS),
                        VERSIONS,
                        KAFKA_VERSION_CHANGE,
                        state,
                        null, SHARED_ENV_PROVIDER);

                String configuration = kc.generatePerBrokerConfiguration(3, advertisedHostnames, advertisedPorts);
                assertThat(configuration, containsString("authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"));
            }
        }
    }
}

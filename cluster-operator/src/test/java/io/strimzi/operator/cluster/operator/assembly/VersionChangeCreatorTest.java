/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class VersionChangeCreatorTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    //////////
    // Tests for a new cluster
    //////////

    @Test
    public void testNewClusterWithAllVersions(VertxTestContext context) {
        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion()),
                mockNewCluster(null, null, List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNewClusterWithoutVersion(VertxTestContext context) {
        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, null, null),
                mockNewCluster(null, null, List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(VERSIONS.defaultVersion().protocolVersion()));
            assertThat(c.logMessageFormatVersion(), is(VERSIONS.defaultVersion().messageVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNewClusterWithKafkaVersionOnly(VertxTestContext context) {
        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), null, null),
                mockNewCluster(null, null, List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(VERSIONS.defaultVersion().protocolVersion()));
            assertThat(c.logMessageFormatVersion(), is(VERSIONS.defaultVersion().messageVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNewClusterWithNewProtocolVersion(VertxTestContext context) {
        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), "3.2", "2.8"),
                mockNewCluster(null, null, List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNewClusterWithOldProtocolVersion(VertxTestContext context) {
        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), "2.8", "2.7"),
                mockNewCluster(null, null, List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    //////////
    // Tests for an existing cluster without upgrade
    //////////

    @Test
    public void testNoopWithAllVersions(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNoopWithoutVersion(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, null, null),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(VERSIONS.defaultVersion().protocolVersion()));
            assertThat(c.logMessageFormatVersion(), is(VERSIONS.defaultVersion().messageVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNoopWithKafkaVersionOnly(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, null, null),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(VERSIONS.defaultVersion().protocolVersion()));
            assertThat(c.logMessageFormatVersion(), is(VERSIONS.defaultVersion().messageVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNoopWithNewProtocolVersion(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = "3.2";
        String logMessageFormatVersion = "2.8";

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNoopWithOldProtocolVersion(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = "2.8";
        String logMessageFormatVersion = "2.7";

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNoopWithAllVersionsFromSts(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        mockSts(kafkaVersion),
                        null,
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNoopWithoutVersionFromSts(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, null, null),
                mockNewCluster(
                        mockSts(kafkaVersion),
                        null,
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(VERSIONS.defaultVersion().protocolVersion()));
            assertThat(c.logMessageFormatVersion(), is(VERSIONS.defaultVersion().messageVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNoopWithAllVersionsFromStsAndSps(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        mockSts("3.0.0"),
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testNoopWithAllVersionsWithoutStsAndSps(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        null,
                        mockUniformPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.requiresZookeeperChange(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    //////////
    // Upgrade tests
    //////////

    @Test
    public void testUpgradeWithAllVersions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithKafkaVersionOnly(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, null, null),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithoutVersions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, null, null),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithOldSubversions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithIVVersions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION + "-IV0";
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION + "-IV0";

        String kafkaVersion = VERSIONS.defaultVersion().version();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithOldPodsAndNewSps(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithMixedPodsAndNewSpsWhenUpgradingKafka(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockMixedPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion, kafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithMixedPodsAndNewSpsWhenUpgradingSubversions(VertxTestContext context) {
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockMixedPods(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion, kafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            // Upgrade is finished, only the protocol versions should be rolled
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithIbpv(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, oldInterBrokerProtocolVersion, null),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(kafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(true));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(nullValue())); // Is null because it is set in the Kafka CR
            assertThat(c.logMessageFormatVersion(), is(oldInterBrokerProtocolVersion)); // Mirrors the inter.broker.protocol.version

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithVeryOldSubversions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = "2.0";
        String oldLogMessageFormatVersion = "2.0";

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = "2.0";
        String logMessageFormatVersion = "2.0";

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testUpgradeFromUnsupportedKafkaVersion(VertxTestContext context) {
        String oldKafkaVersion = "2.8.0";
        String oldInterBrokerProtocolVersion = "2.8";
        String oldLogMessageFormatVersion = "2.8";

        String kafkaVersion = VERSIONS.defaultVersion().version();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, null, null),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeFromUnsupportedKafkaVersionWithAllVersions(VertxTestContext context) {
        String oldKafkaVersion = "2.8.0";
        String oldInterBrokerProtocolVersion = "2.8";
        String oldLogMessageFormatVersion = "2.8";

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithKubernetesResourcesWithoutVersions(VertxTestContext context) {
        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(null),
                        mockUniformPods(null, null, null)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c.getClass(), is(KafkaUpgradeException.class));
            assertThat(c.getMessage(), is("Kafka Pods or StatefulSet exist, but do not contain the strimzi.io/kafka-version annotation to detect their version. Kafka upgrade cannot be detected."));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeFromStatefulSet(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = VERSIONS.defaultVersion().version();
        String interBrokerProtocolVersion = VERSIONS.defaultVersion().protocolVersion();
        String logMessageFormatVersion = VERSIONS.defaultVersion().messageVersion();

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        mockSts(oldKafkaVersion),
                        null,
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(false));
            assertThat(c.isUpgrade(), is(true));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    //////////
    // Downgrade tests
    //////////

    @Test
    public void testDowngradeWithAllVersions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String interBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String logMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(true));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.version(kafkaVersion)));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithoutSubversionsButWithOldSubversionsInKubeResources(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, null, null),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(true));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.version(kafkaVersion)));
            assertThat(c.interBrokerProtocolVersion(), is(oldInterBrokerProtocolVersion));
            assertThat(c.logMessageFormatVersion(), is(oldLogMessageFormatVersion));

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithOlderSubversions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = "2.8";
        String oldLogMessageFormatVersion = "2.8";

        String kafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String interBrokerProtocolVersion = "2.8";
        String logMessageFormatVersion = "2.8";

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(true));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.version(kafkaVersion)));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithAllVersionsAndRecovery(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        String kafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String interBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String logMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(kafkaVersion),
                        mockMixedPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion, kafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.isNoop(), is(false));
            assertThat(c.isDowngrade(), is(true));
            assertThat(c.isUpgrade(), is(false));
            assertThat(c.from(), is(VERSIONS.version(oldKafkaVersion)));
            assertThat(c.to(), is(VERSIONS.version(kafkaVersion)));
            assertThat(c.interBrokerProtocolVersion(), nullValue());
            assertThat(c.logMessageFormatVersion(), nullValue());

            async.flag();
        })));
    }

    // Everything already to the new protocol version => downgrade should not be possible
    @Test
    public void testDowngradeFailsWithNewProtocolVersions(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.LATEST_FORMAT_VERSION;

        String kafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String interBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String logMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockUniformPods(oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c.getClass(), is(KafkaUpgradeException.class));
            assertThat(c.getMessage(), is("log.message.format.version (" + oldInterBrokerProtocolVersion + ") and inter.broker.protocol.version (" + oldLogMessageFormatVersion + ") used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to (" + KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + ")"));

            async.flag();
        })));
    }

    // Some pods were already rolled to the new protocol version => downgrade should not be possible
    @Test
    public void testDowngradeFailsWithNewProtocolVersionInOnePod(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.LATEST_FORMAT_VERSION;

        String kafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String interBrokerProtocolVersion = KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION;
        String logMessageFormatVersion = KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION;

        VersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                mockNewCluster(
                        null,
                        mockSps(oldKafkaVersion),
                        mockMixedPods(oldKafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion, oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                )
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c.getClass(), is(KafkaUpgradeException.class));
            assertThat(c.getMessage(), is("log.message.format.version (" + oldInterBrokerProtocolVersion + ") and inter.broker.protocol.version (" + oldLogMessageFormatVersion + ") used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to (" + KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + ")"));

            async.flag();
        })));
    }

    @Test
    public void testDowngradeFailsWithUnsupportedVersion(VertxTestContext context) {
        String oldKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String oldInterBrokerProtocolVersion = KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION;
        String oldLogMessageFormatVersion = KafkaVersionTestUtils.LATEST_FORMAT_VERSION;

        String kafkaVersion = "2.8.0";
        String interBrokerProtocolVersion = "2.8";
        String logMessageFormatVersion = "2.8";

        Checkpoint async = context.checkpoint();

        Exception e = Assertions.assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            mockVersionChangeCreator(
                    mockKafka(kafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion),
                    mockNewCluster(
                            null,
                            mockSps(oldKafkaVersion),
                            mockMixedPods(oldKafkaVersion, interBrokerProtocolVersion, logMessageFormatVersion, oldKafkaVersion, oldInterBrokerProtocolVersion, oldLogMessageFormatVersion)
                    )
            );
        });

        assertThat(e.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.8.0. Supported versions are"));

        async.flag();
    }

    //////////
    // Utility methods used during the tests
    //////////

    // Creates the VersionChangeCreator with the mocks
    private VersionChangeCreator mockVersionChangeCreator(Kafka kafka, ResourceOperatorSupplier ros)  {
        return new VersionChangeCreator(new Reconciliation("test", "Kafka", NAMESPACE, CLUSTER_NAME), kafka, ResourceUtils.dummyClusterOperatorConfig(), ros);
    }

    // Creates ResourceOperatorSupplier with mocks
    private ResourceOperatorSupplier mockNewCluster(StatefulSet sts, StrimziPodSet sps, List<Pod> pods)   {
        ResourceOperatorSupplier ros = ResourceUtils.supplierWithMocks(false);

        StatefulSetOperator stsOps = ros.stsOperations;
        when(stsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(sts));

        StrimziPodSetOperator spsOps = ros.strimziPodSetOperator;
        when(spsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(sps));

        PodOperator podOps = ros.podOperations;
        when(podOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        return ros;
    }

    // Internal method used to add an option to the Kafka CR .spec.kafka.config section which creates the new Map if needed
    private void updateConfig(Kafka kafka, String configKey, String configValue)    {
        Map<String, Object> config = kafka.getSpec().getKafka().getConfig();

        if (config != null) {
            config.put(configKey, configValue);
        } else {
            config = new HashMap<>();
            config.put(configKey, configValue);
            kafka.getSpec().getKafka().setConfig(config);
        }
    }

    // Prepares the Kafka CR with the specified versions
    private Kafka mockKafka(String kafkaVersion, String interBrokerProtocolVersion, String logMessageFormatVersion)  {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        if (kafkaVersion != null)   {
            kafka.getSpec().getKafka().setVersion(kafkaVersion);
        }

        if (interBrokerProtocolVersion != null)   {
            updateConfig(kafka, KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, interBrokerProtocolVersion);
        }

        if (logMessageFormatVersion != null)   {
            updateConfig(kafka, KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion);
        }

        return kafka;
    }

    // Prepares the StatefulSet with the specified versions
    private StatefulSet mockSts(String kafkaVersion)  {
        StatefulSet sts = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME  + "-kafka")
                .withAnnotations(new HashMap<>())
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();

        if (kafkaVersion != null)   {
            sts.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion);
        }

        return sts;
    }

    // Prepares the StrimziPodSet with the specified versions
    private StrimziPodSet mockSps(String kafkaVersion)  {
        StrimziPodSet sps = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(CLUSTER_NAME  + "-kafka")
                    .withAnnotations(new HashMap<>())
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();

        if (kafkaVersion != null)   {
            sps.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion);
        }

        return sps;
    }

    // Prepares the Pods all with the same versions
    private List<Pod> mockUniformPods(String kafkaVersion, String interBrokerProtocolVersion, String logMessageFormatVersion)  {
        List<Pod> pods = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(NAMESPACE)
                        .withName(CLUSTER_NAME  + "-kafka-" + i)
                        .withAnnotations(new HashMap<>())
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build();

            if (kafkaVersion != null)   {
                pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion);
            }

            if (interBrokerProtocolVersion != null)   {
                pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, interBrokerProtocolVersion);
            }

            if (logMessageFormatVersion != null)   {
                pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion);
            }

            pods.add(pod);
        }

        return pods;
    }

    // Prepares the Pods all with mixed versions
    private List<Pod> mockMixedPods(String kafkaVersion, String interBrokerProtocolVersion, String logMessageFormatVersion,
                                    String kafkaVersion2, String interBrokerProtocolVersion2, String logMessageFormatVersion2)  {
        List<Pod> pods = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(NAMESPACE)
                        .withName(CLUSTER_NAME  + "-kafka-" + i)
                        .withAnnotations(new HashMap<>())
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build();

            if (i % 2 == 0) {
                if (kafkaVersion != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion);
                }

                if (interBrokerProtocolVersion != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, interBrokerProtocolVersion);
                }

                if (logMessageFormatVersion != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion);
                }
            } else {
                if (kafkaVersion2 != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion2);
                }

                if (interBrokerProtocolVersion2 != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, interBrokerProtocolVersion2);
                }

                if (logMessageFormatVersion2 != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion2);
                }
            }

            pods.add(pod);
        }

        return pods;
    }
}

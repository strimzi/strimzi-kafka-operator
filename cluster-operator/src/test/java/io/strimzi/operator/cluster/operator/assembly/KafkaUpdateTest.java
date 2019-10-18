/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_FROM_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_TO_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION;
import static io.strimzi.test.TestUtils.map;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaUpdateTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private Vertx vertx = Vertx.vertx();

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    public static EnvVar findEnv(List<EnvVar> env, String envVar) {
        EnvVar value = null;
        for (EnvVar e : env) {
            if (envVar.equals(e.getName())) {
                value = e;
            }
        }
        return value;
    }

    private Kafka initialKafka(String version, Map<String, Object> config) {
        return new KafkaBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(NAME)
                        .withNamespace(NAMESPACE)
                        .build())
                .withNewSpec()
                .withNewKafka()
                .withReplicas(2)
                .withVersion(version)
                .withConfig(config)
                .withNewEphemeralStorage().endEphemeralStorage()
                .endKafka()
                .withNewZookeeper()
                .withReplicas(1)
                .withNewEphemeralStorage().endEphemeralStorage()
                .endZookeeper()
                .withNewTopicOperator()
                .endTopicOperator()
                .endSpec()
                .build();
    }

    private Kafka upgradedKafka(Kafka initialKafka, String version, Map<String, Object> config) {
        return new KafkaBuilder(initialKafka)
                .editSpec()
                .editKafka()
                .withVersion(version)
                .withConfig(config)
                .endKafka()
                .endSpec()
                .build();
    }

    static class UpgradeException extends RuntimeException {
        public final List<StatefulSet> states;

        public UpgradeException(List<StatefulSet> states, Throwable cause) {
            super(cause);
            this.states = states;
        }
    }

    private List<StatefulSet> upgrade(TestContext context, Map<String, String> versionMap,
                                      Kafka initialKafka, StatefulSet initialSs, Kafka updatedKafka,
                                      Consumer<Integer> reconcileExceptions, Consumer<Integer> rollExceptions) {
        KafkaSetOperator kso = mock(KafkaSetOperator.class);

        StatefulSet kafkaSs = initialSs != null ? initialSs : KafkaCluster.fromCrd(initialKafka, VERSIONS).generateStatefulSet(false, null, null);

        when(kso.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kafkaSs));

        List<StatefulSet> states = new ArrayList<>(2);
        when(kso.reconcile(anyString(), anyString(), any(StatefulSet.class))).thenAnswer(invocation -> {
            reconcileExceptions.accept(states.size());
            StatefulSet ss = invocation.getArgument(2);
            states.add(new StatefulSetBuilder(ss).build());
            return Future.succeededFuture(ReconcileResult.patched(new StatefulSetBuilder(ss).build()));
        });

        AtomicInteger rollingUpdates = new AtomicInteger();
        when(kso.maybeRollingUpdate(any(), any())).thenAnswer(invocation -> {
            //context.assertTrue(((Predicate<Pod>) invocation.getArgument(1)).test(pod));
            rollExceptions.accept(rollingUpdates.getAndIncrement());
            return Future.succeededFuture();
        });

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_9),
                new MockCertManager(),
                new ResourceOperatorSupplier(null, null, null,
                        kso, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null, null, null),
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS, 1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Async async = context.async();
        Future<KafkaAssemblyOperator.ReconciliationState> future = op
                .new ReconciliationState(reconciliation, updatedKafka) {
                    @Override
                    public Future<Void> waitForQuiescence(StatefulSet ss) {
                        return Future.succeededFuture();
                    }
                }
                .kafkaUpgrade();
        AtomicReference<UpgradeException> ex = new AtomicReference<>();
        future.setHandler(ar -> {
            if (ar.failed()) {
                ex.set(new UpgradeException(states, ar.cause()));
            }
            async.complete();
        });
        async.await();
        if (ex.get() != null) {
            throw ex.get();
        }
        return states;
    }

    @Test
    public void upgradeMinorToPrevWithEmptyConfig(TestContext context) throws IOException {
        try {
            testUpgradeMinorToPrevMessageFormatConfig(context, emptyMap(), true);
        } catch (UpgradeException e) {
            context.assertTrue(e.getCause() instanceof KafkaUpgradeException);
            context.assertTrue(e.states.isEmpty());
        }
    }

    @Test
    public void upgradeMinorToPrevWithSameMessageFormatConfig(TestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_MINOR_PROTOCOL_VERSION),
                // Minor version upgrade doesn't require proto or mvg version change, so single phase
                true);
    }

    @Test
    public void upgradeMinorToPrevWithOldMessageFormatConfig(TestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION, "1.0"), true);
    }

    @Test
    public void upgradeMinorToPrevWithSameProtocolVersion(TestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION,
                        INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_PROTOCOL_VERSION),
                // Minor version upgrade doesn't require proto or mvg version change, so single phase
                true);
    }

    @Test
    public void upgradeMinorToPrevWithOldProtocolVersion(TestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION,
                        INTERBROKER_PROTOCOL_VERSION, "1.1"),
                // Minor version upgrade doesn't require proto or mvg version change, so single phase
                true);
    }

    private void testUpgradeMinorToPrevMessageFormatConfig(TestContext context, Map<String, Object> config, boolean expectSinglePhase) throws IOException {
        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_MINOR_KAFKA_VERSION;
        String upgradedKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String upgradedImage = KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE;
        Kafka initialKafka = initialKafka(initialKafkaVersion, config);
        List<StatefulSet> states = upgrade(context,
                singletonMap(upgradedKafkaVersion, upgradedImage),
                initialKafka, null,
                upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });
        context.assertEquals(expectSinglePhase ? 1 : 2, states.size());

        if (!expectSinglePhase) {
            StatefulSet phase1 = states.get(0);
            context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
            context.assertEquals(initialKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION));
            context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION));
            Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
            context.assertEquals(upgradedImage, container1.getImage());
            List<EnvVar> env = container1.getEnv();
            KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env, ENV_VAR_KAFKA_CONFIGURATION).getValue());
            context.assertEquals(config.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_PROTOCOL_VERSION),
                    config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
            context.assertEquals(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION),
                    config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION));
        }

        StatefulSet phase2 = states.get(expectSinglePhase ? 0 : 1);
        context.assertEquals(upgradedKafkaVersion, phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.assertEquals(upgradedImage, container2.getImage());
        List<EnvVar> env2 = container2.getEnv();
        EnvVar env = findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION);
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(env != null ? env.getValue() : "");
        context.assertEquals(config.get(INTERBROKER_PROTOCOL_VERSION), config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
        context.assertEquals(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION),
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                "Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value");
    }

    /** Test we can recover from an exception during phase 1 rolling of the upgrade */
    @Test
    public void testUpgradeMinorToPrevMessageFormatConfig_exceptionDuringPhase0Roll(TestContext context) throws IOException {
        Map<String, Object> initialConfig = singletonMap(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION);
        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_MINOR_KAFKA_VERSION;
        String upgradedKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String upgradedImage = KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE;
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        Kafka initialKafka = initialKafka(initialKafkaVersion, initialConfig);

        // Do an upgrade, but make the rolling update throw
        List<StatefulSet> states = null;
        try {
            upgrade(context,
                    singletonMap(upgradedKafkaVersion, upgradedImage),
                    initialKafka, null,
                    upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
                invocationCount -> {
                },
                invocationCount -> {
                    if (invocationCount == 0
                            && exceptionThrown.compareAndSet(false, true)) {
                        throw new RuntimeException("Testing exception during roll");
                    }
                });
            context.fail();
        } catch (UpgradeException e) {
            context.assertTrue(e.getCause() instanceof RuntimeException, e.getCause().getMessage());
            states = e.states;
        }

        context.assertTrue(exceptionThrown.get());
        context.assertEquals(1, states.size());
        StatefulSet phase1 = states.get(0);
        context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
        context.assertNull(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION));
        context.assertNull(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION));
        Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.assertEquals(upgradedImage, container1.getImage());
        List<EnvVar> env1 = container1.getEnv();
        KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env1, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.assertNull(config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
        context.assertEquals(initialConfig.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION),
                config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION));

        // Do the upgrade again, but without throwing this time
        states = upgrade(context,
                singletonMap(upgradedKafkaVersion, upgradedImage),
                initialKafka, states.get(0),
                upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });

        // TODO Need to assert that the pods get rolled in this 2nd attempt before the start of phase 2

        // We expect the only observer reconcile() state to be from phase 2 (i.e. we didn't repeat phase 1)
        context.assertEquals(0, states.size());
    }

    /////////////////

    @Test
    public void upgradePrevToLatestWithEmptyConfig(TestContext context) throws IOException {
        try {
            testUpgradePrevToLatestMessageFormatConfig(context, emptyMap(), false);
        } catch (UpgradeException e) {
            context.assertTrue(e.getCause() instanceof KafkaUpgradeException);
            context.assertTrue(e.states.isEmpty());
        }
    }

    @Test
    public void upgradePrevToLatestWithPrevMessageFormatConfig(TestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION), false);
    }

    @Test
    public void upgradePrevToLatestWithOldMessageFormatConfig(TestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                "1.0"), false);
    }

    @Test
    public void upgradePrevToLatestWithPrevProtocolVersion(TestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION), true);
    }

    @Test
    public void upgradePrevToLatestWithOldProtocolVersion(TestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                        INTERBROKER_PROTOCOL_VERSION, "1.1"), true);
    }

    private void testUpgradePrevToLatestMessageFormatConfig(TestContext context, Map<String, Object> config, boolean expectSinglePhase) throws IOException {
        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String upgradedKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String upgradedImage = KafkaVersionTestUtils.LATEST_KAFKA_IMAGE;
        Kafka initialKafka = initialKafka(initialKafkaVersion, config);
        List<StatefulSet> states = upgrade(context,
            singletonMap(upgradedKafkaVersion, upgradedImage),
            initialKafka, null,
            upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });
        context.assertEquals(expectSinglePhase ? 1 : 2, states.size());

        if (!expectSinglePhase) {
            StatefulSet phase1 = states.get(0);
            context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
            context.assertEquals(initialKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION));
            context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION));
            Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
            context.assertEquals(upgradedImage, container1.getImage());
            List<EnvVar> env = container1.getEnv();
            KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env, ENV_VAR_KAFKA_CONFIGURATION).getValue());
            context.assertEquals(config.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION),
                    config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
            context.assertEquals(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                    config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION));
        }

        StatefulSet phase2 = states.get(expectSinglePhase ? 0 : 1);
        context.assertEquals(upgradedKafkaVersion, phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.assertEquals(upgradedImage, container2.getImage());
        List<EnvVar> env2 = container2.getEnv();
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.assertEquals(config.get(INTERBROKER_PROTOCOL_VERSION), config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
        context.assertEquals(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                "Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value");
    }

    /** Test we can recover from an exception during phase 1 rolling of the upgrade */
    @Test
    public void testUpgradePrevToLatestMessageFormatConfig_exceptionDuringPhase0Roll(TestContext context) throws IOException {
        Map<String, Object> config = singletonMap(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION);
        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String upgradedKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String upgradedImage = KafkaVersionTestUtils.LATEST_KAFKA_IMAGE;
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        Kafka initialKafka = initialKafka(initialKafkaVersion, config);

        // Do an upgrade, but make the rolling update throw
        List<StatefulSet> states = null;
        try {
            upgrade(context,
                singletonMap(upgradedKafkaVersion, upgradedImage),
                initialKafka, null,
                upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
                invocationCount -> {
                },
                invocationCount -> {
                    if (invocationCount == 0
                            && exceptionThrown.compareAndSet(false, true)) {
                        throw new RuntimeException("Testing exception during roll");
                    }
                });
            context.fail();
        } catch (UpgradeException e) {
            context.assertTrue(e.getCause() instanceof RuntimeException, e.getCause().getMessage());
            states = e.states;
        }

        context.assertTrue(exceptionThrown.get());
        context.assertEquals(1, states.size());
        StatefulSet phase1 = states.get(0);
        context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
        context.assertEquals(initialKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION));
        context.assertEquals(upgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION));
        Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.assertEquals(upgradedImage, container1.getImage());
        List<EnvVar> env1 = container1.getEnv();
        KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env1, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.assertEquals(config.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION),
                config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
        context.assertEquals(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION));

        // Do the upgrade again, but without throwing this time
        states = upgrade(context,
            singletonMap(upgradedKafkaVersion, upgradedImage),
            initialKafka, states.get(0),
            upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });

        // TODO Need to assert that the pods get rolled in this 2nd attempt before the start of phase 2

        // We expect the only observer reconcile() state to be from phase 2 (i.e. we didn't repeat phase 1)
        context.assertEquals(1, states.size());

        StatefulSet phase2 = states.get(0);
        context.assertEquals(upgradedKafkaVersion, phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.assertEquals(upgradedImage, container2.getImage());
        List<EnvVar> env2 = container2.getEnv();
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.assertEquals(null, config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
        context.assertEquals(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                "Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value");
    }

    @Test
    public void downgradeLatestToPrevWithEmptyConfig(TestContext context) throws IOException {
        try {
            testDowngradeLatestToPrevMessageFormatConfig(context, emptyMap(), true);
            context.fail();
        } catch (UpgradeException e) {
            context.assertTrue(e.getCause() instanceof KafkaUpgradeException);
            context.assertTrue(e.states.isEmpty());
        }
    }

    @Test
    public void downgradeLatestToPrevWithLatestMessageFormatConfig(TestContext context) throws IOException {
        try {
            testDowngradeLatestToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                    KafkaVersionTestUtils.LATEST_FORMAT_VERSION), true);
            context.fail();
        } catch (UpgradeException e) {
            context.assertTrue(e.getCause() instanceof KafkaUpgradeException);
            context.assertTrue(e.states.isEmpty());
        }
    }

    @Test
    public void downgradeLatestToPrevWithPrevMessageFormatConfig(TestContext context) throws IOException {
        testDowngradeLatestToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION), true);
    }

    @Test
    public void downgradeLatestToPrevWithLatestProtocolVersion(TestContext context) throws IOException {
        testDowngradeLatestToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION),
                true);
    }

    @Test
    public void downgradeLatestToPrevWithPrevProtocolVersion(TestContext context) throws IOException {
        testDowngradeLatestToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION),
                false);
    }

    private void testDowngradeLatestToPrevMessageFormatConfig(TestContext context, Map<String, Object> initialConfig, boolean expectFirstPhase) throws IOException {
        String initialKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String downgradedKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String downgradedImage = KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE;
        Kafka initialKafka = initialKafka(initialKafkaVersion, initialConfig);
        List<StatefulSet> states = upgrade(context,
            singletonMap(downgradedKafkaVersion, downgradedImage),
            initialKafka, null,
            upgradedKafka(initialKafka, downgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });
        context.assertEquals(expectFirstPhase ? 2 : 1, states.size());

        if (expectFirstPhase) {
            StatefulSet phase1 = states.get(0);
            context.assertEquals(initialKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
            context.assertEquals(initialKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION));
            context.assertEquals(downgradedKafkaVersion, phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION));
            Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
            context.assertNotEquals(downgradedImage, container1.getImage());
            List<EnvVar> env1 = container1.getEnv();
            KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env1, ENV_VAR_KAFKA_CONFIGURATION).getValue());
            context.assertEquals(initialConfig.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION),
                    config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
            context.assertEquals(initialConfig.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                    config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION));
        }

        StatefulSet phase2 = states.get(expectFirstPhase ? 1 : 0);
        context.assertEquals(downgradedKafkaVersion, phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION));
        context.assertFalse(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.assertEquals(downgradedImage, container2.getImage());
        List<EnvVar> env2 = container2.getEnv();
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.assertEquals(null, config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION));
        context.assertEquals(initialConfig.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                "Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value");
    }

}

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
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
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

    private List<StatefulSet> upgrade(VertxTestContext context, Map<String, String> versionMap,
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
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                new ResourceOperatorSupplier(null, null, null,
                        kso, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null, null, null, null),
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS, 1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
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
            async.flag();
        });
        ex.get();
        if (ex.get() != null) {
            throw ex.get();
        }
        return states;
    }

    @Test
    public void upgradeMinorToPrevWithEmptyConfig(VertxTestContext context) throws IOException {
        try {
            testUpgradeMinorToPrevMessageFormatConfig(context, emptyMap(), true);
        } catch (UpgradeException e) {
            context.verify(() -> assertThat(e.getCause() instanceof KafkaUpgradeException, is(true)));
            context.verify(() -> assertThat(e.states.isEmpty(), is(true)));
        }
    }

    @Test
    public void upgradeMinorToPrevWithSameMessageFormatConfig(VertxTestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_MINOR_PROTOCOL_VERSION),
                // Minor version upgrade doesn't require proto or mvg version change, so single phase
                true);
    }

    @Test
    public void upgradeMinorToPrevWithOldMessageFormatConfig(VertxTestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION, "1.0"), true);
    }

    @Test
    public void upgradeMinorToPrevWithSameProtocolVersion(VertxTestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION,
                        INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_PROTOCOL_VERSION),
                // Minor version upgrade doesn't require proto or mvg version change, so single phase
                true);
    }

    @Test
    public void upgradeMinorToPrevWithOldProtocolVersion(VertxTestContext context) throws IOException {
        testUpgradeMinorToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION,
                        INTERBROKER_PROTOCOL_VERSION, "1.1"),
                // Minor version upgrade doesn't require proto or mvg version change, so single phase
                true);
    }

    private void testUpgradeMinorToPrevMessageFormatConfig(VertxTestContext context, Map<String, Object> config, boolean expectSinglePhase) throws IOException {
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
        context.verify(() -> assertThat(states.size(), is(expectSinglePhase ? 1 : 2)));

        if (!expectSinglePhase) {
            StatefulSet phase1 = states.get(0);
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION), is(initialKafkaVersion)));
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION), is(upgradedKafkaVersion)));
            Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
            context.verify(() -> assertThat(container1.getImage(), is(upgradedImage)));
            List<EnvVar> env = container1.getEnv();
            KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env, ENV_VAR_KAFKA_CONFIGURATION).getValue());
            context.verify(() -> assertThat(config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION),
                    is(config.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_PROTOCOL_VERSION))));
            context.verify(() -> assertThat(config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                    is(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION))));
        }

        StatefulSet phase2 = states.get(expectSinglePhase ? 0 : 1);
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION), is(false)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION), is(false)));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.verify(() -> assertThat(container2.getImage(), is(upgradedImage)));
        List<EnvVar> env2 = container2.getEnv();
        EnvVar env = findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION);
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(env != null ? env.getValue() : "");
        context.verify(() -> assertThat(config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION), is(config.get(INTERBROKER_PROTOCOL_VERSION))));
        context.verify(() -> assertThat("Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value",
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION),
                is(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION))));
    }

    /** Test we can recover from an exception during phase 1 rolling of the upgrade */
    @Test
    public void testUpgradeMinorToPrevMessageFormatConfig_exceptionDuringPhase0Roll(VertxTestContext context) throws IOException {
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
            context.failNow(new Throwable());
        } catch (UpgradeException e) {
            context.verify(() -> assertThat(e.getCause().getMessage(), e.getCause() instanceof RuntimeException, is(true)));
            states = e.states;
        }

        context.verify(() -> assertThat(exceptionThrown.get(), is(true)));
        List<StatefulSet> finalStates1 = states;
        context.verify(() -> assertThat(finalStates1.size(), is(1)));
        StatefulSet phase1 = states.get(0);
        context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
        context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION), is(nullValue())));
        context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION), is(nullValue())));
        Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.verify(() -> assertThat(container1.getImage(), is(upgradedImage)));
        List<EnvVar> env1 = container1.getEnv();
        KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env1, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.verify(() -> assertThat(config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION), is(nullValue())));
        context.verify(() -> assertThat(config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                is(initialConfig.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_MINOR_FORMAT_VERSION))));

        // Do the upgrade again, but without throwing this time
        states = upgrade(context,
                singletonMap(upgradedKafkaVersion, upgradedImage),
                initialKafka, states.get(0),
                upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });

        // TODO Need to assert that the pods get rolled in this 2nd attempt before the start of phase 2

        // We expect the only observer reconcile() state to be from phase 2 (i.e. we didn't repeat phase 1)
        List<StatefulSet> finalStates = states;
        context.verify(() -> assertThat(finalStates.size(), is(0)));
    }

    /////////////////

    @Test
    public void upgradePrevToLatestWithEmptyConfig(VertxTestContext context) throws IOException {
        try {
            testUpgradePrevToLatestMessageFormatConfig(context, emptyMap(), false);
        } catch (UpgradeException e) {
            context.verify(() -> assertThat(e.getCause() instanceof KafkaUpgradeException, is(true)));
            context.verify(() -> assertThat(e.states.isEmpty(), is(true)));
        }
    }

    @Test
    public void upgradePrevToLatestWithPrevMessageFormatConfig(VertxTestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION), false);
    }

    @Test
    public void upgradePrevToLatestWithOldMessageFormatConfig(VertxTestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                "1.0"), false);
    }

    @Test
    public void upgradePrevToLatestWithPrevProtocolVersion(VertxTestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION), true);
    }

    @Test
    public void upgradePrevToLatestWithOldProtocolVersion(VertxTestContext context) throws IOException {
        testUpgradePrevToLatestMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                        INTERBROKER_PROTOCOL_VERSION, "1.1"), true);
    }

    private void testUpgradePrevToLatestMessageFormatConfig(VertxTestContext context, Map<String, Object> config, boolean expectSinglePhase) throws IOException {
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
        context.verify(() -> assertThat(states.size(), is(expectSinglePhase ? 1 : 2)));

        if (!expectSinglePhase) {
            StatefulSet phase1 = states.get(0);
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION), is(initialKafkaVersion)));
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION), is(upgradedKafkaVersion)));
            Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
            context.verify(() -> assertThat(container1.getImage(), is(upgradedImage)));
            List<EnvVar> env = container1.getEnv();
            KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env, ENV_VAR_KAFKA_CONFIGURATION).getValue());
            context.verify(() -> assertThat(config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION),
                    is(config.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION))));
            context.verify(() -> assertThat(config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                    is(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION))));
        }

        StatefulSet phase2 = states.get(expectSinglePhase ? 0 : 1);
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION), is(false)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION), is(false)));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.verify(() -> assertThat(container2.getImage(), is(upgradedImage)));
        List<EnvVar> env2 = container2.getEnv();
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.verify(() -> assertThat(config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION), is(config.get(INTERBROKER_PROTOCOL_VERSION))));
        context.verify(() -> assertThat("Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value",
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                is(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION))));
    }

    /** Test we can recover from an exception during phase 1 rolling of the upgrade */
    @Test
    public void testUpgradePrevToLatestMessageFormatConfig_exceptionDuringPhase0Roll(VertxTestContext context) throws IOException {
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
            context.failNow(new Throwable());
        } catch (UpgradeException e) {
            context.verify(() -> assertThat(e.getCause().getMessage(), e.getCause() instanceof RuntimeException, is(true)));
            states = e.states;
        }

        context.verify(() -> assertThat(exceptionThrown.get(), is(true)));
        List<StatefulSet> finalStates1 = states;
        context.verify(() -> assertThat(finalStates1.size(), is(1)));
        StatefulSet phase1 = states.get(0);
        context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
        context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION), is(initialKafkaVersion)));
        context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION), is(upgradedKafkaVersion)));
        Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.verify(() -> assertThat(container1.getImage(), is(upgradedImage)));
        List<EnvVar> env1 = container1.getEnv();
        KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env1, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.verify(() -> assertThat(config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION),
                is(config.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION))));
        context.verify(() -> assertThat(config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                is(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION))));

        // Do the upgrade again, but without throwing this time
        states = upgrade(context,
            singletonMap(upgradedKafkaVersion, upgradedImage),
            initialKafka, states.get(0),
            upgradedKafka(initialKafka, upgradedKafkaVersion, emptyMap()),
            invocationCount -> { },
            invocationCount -> { });

        // TODO Need to assert that the pods get rolled in this 2nd attempt before the start of phase 2

        // We expect the only observer reconcile() state to be from phase 2 (i.e. we didn't repeat phase 1)
        List<StatefulSet> finalStates = states;
        context.verify(() -> assertThat(finalStates.size(), is(1)));

        StatefulSet phase2 = states.get(0);
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(upgradedKafkaVersion)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION), is(false)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION), is(false)));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.verify(() -> assertThat(container2.getImage(), is(upgradedImage)));
        List<EnvVar> env2 = container2.getEnv();
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.verify(() -> assertThat(config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION), is(nullValue())));
        context.verify(() -> assertThat("Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value",
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                is(config.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION))));
    }

    @Test
    public void downgradeLatestToPrevWithEmptyConfig(VertxTestContext context) throws IOException {
        try {
            testDowngradeLatestToPrevMessageFormatConfig(context, emptyMap(), true);
            context.failNow(new Throwable());
        } catch (UpgradeException e) {
            context.verify(() -> assertThat(e.getCause() instanceof KafkaUpgradeException, is(true)));
            context.verify(() -> assertThat(e.states.isEmpty(), is(true)));
        }
    }

    @Test
    public void downgradeLatestToPrevWithLatestMessageFormatConfig(VertxTestContext context) throws IOException {
        try {
            testDowngradeLatestToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                    KafkaVersionTestUtils.LATEST_FORMAT_VERSION), true);
            context.failNow(new Throwable());
        } catch (UpgradeException e) {
            context.verify(() -> assertThat(e.getCause() instanceof KafkaUpgradeException, is(true)));
            context.verify(() -> assertThat(e.states.isEmpty(), is(true)));
        }
    }

    @Test
    public void downgradeLatestToPrevWithPrevMessageFormatConfig(VertxTestContext context) throws IOException {
        testDowngradeLatestToPrevMessageFormatConfig(context, singletonMap(LOG_MESSAGE_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION), true);
    }

    @Test
    public void downgradeLatestToPrevWithLatestProtocolVersion(VertxTestContext context) throws IOException {
        testDowngradeLatestToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION),
                true);
    }

    @Test
    public void downgradeLatestToPrevWithPrevProtocolVersion(VertxTestContext context) throws IOException {
        testDowngradeLatestToPrevMessageFormatConfig(context,
                (Map) map(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION),
                false);
    }

    private void testDowngradeLatestToPrevMessageFormatConfig(VertxTestContext context, Map<String, Object> initialConfig, boolean expectFirstPhase) throws IOException {
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
        context.verify(() -> assertThat(states.size(), is(expectFirstPhase ? 2 : 1)));

        if (expectFirstPhase) {
            StatefulSet phase1 = states.get(0);
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(initialKafkaVersion)));
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FROM_VERSION), is(initialKafkaVersion)));
            context.verify(() -> assertThat(phase1.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_TO_VERSION), is(downgradedKafkaVersion)));
            Container container1 = phase1.getSpec().getTemplate().getSpec().getContainers().get(0);
            context.verify(() -> assertThat(container1.getImage(), is(not(downgradedImage))));
            List<EnvVar> env1 = container1.getEnv();
            KafkaConfiguration config1 = KafkaConfiguration.unvalidated(findEnv(env1, ENV_VAR_KAFKA_CONFIGURATION).getValue());
            context.verify(() -> assertThat(config1.getConfigOption(INTERBROKER_PROTOCOL_VERSION),
                    is(initialConfig.getOrDefault(INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION))));
            context.verify(() -> assertThat(config1.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                    is(initialConfig.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION))));
        }

        StatefulSet phase2 = states.get(expectFirstPhase ? 1 : 0);
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_KAFKA_VERSION), is(downgradedKafkaVersion)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_FROM_VERSION), is(false)));
        context.verify(() -> assertThat(phase2.getMetadata().getAnnotations().containsKey(ANNO_STRIMZI_IO_TO_VERSION), is(false)));
        Container container2 = phase2.getSpec().getTemplate().getSpec().getContainers().get(0);
        context.verify(() -> assertThat(container2.getImage(), is(downgradedImage)));
        List<EnvVar> env2 = container2.getEnv();
        KafkaConfiguration config2 = KafkaConfiguration.unvalidated(findEnv(env2, ENV_VAR_KAFKA_CONFIGURATION).getValue());
        context.verify(() -> assertThat(config2.getConfigOption(INTERBROKER_PROTOCOL_VERSION), is(nullValue())));
        context.verify(() -> assertThat("Expect the log.message.format.version to be unchanged from configured or default (for kafka " + initialKafkaVersion + ") value",
                config2.getConfigOption(LOG_MESSAGE_FORMAT_VERSION),
                is(initialConfig.getOrDefault(LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION))));
    }
}

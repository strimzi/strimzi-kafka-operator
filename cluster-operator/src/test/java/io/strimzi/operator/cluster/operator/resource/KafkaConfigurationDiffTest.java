/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.ReadWriteUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaConfigurationDiffTest {
    KafkaVersion kafkaVersion = KafkaVersionTestUtils.getKafkaVersionLookup().defaultVersion();
    private final NodeRef brokerNodeRef = new NodeRef("broker-0", 0, "broker", false, true);
    private final NodeRef controllerNodeRef = new NodeRef("controller-1", 1, "controller", true, false);
    private final NodeRef combinedNodeRef = new NodeRef("combined-2", 2, "combined", true, true);

    private ConfigEntry instantiateConfigEntry(String name, String val) {
        // use reflection to instantiate ConfigEntry
        Constructor<?> constructor;
        ConfigEntry configEntry = null;
        {
            try {
                constructor = ConfigEntry.class.getDeclaredConstructor(String.class, String.class, ConfigEntry.ConfigSource.class, boolean.class, boolean.class, List.class, ConfigEntry.ConfigType.class, String.class);
                constructor.setAccessible(true);
                configEntry = (ConfigEntry) constructor.newInstance(name, val, ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false, emptyList(), ConfigEntry.ConfigType.STRING, "doc");
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                fail();
            }
        }
        return configEntry;
    }

    private String getDesiredConfiguration(List<ConfigEntry> additional) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("desired-kafka-broker.conf")) {
            StringBuilder desiredConfigString = new StringBuilder(ReadWriteUtils.readInputStream(is));

            for (ConfigEntry ce : additional) {
                desiredConfigString.append("\n").append(ce.name()).append("=").append(ce.value());
            }

            return desiredConfigString.toString();
        } catch (IOException e) {
            fail(e);
        }
        return "";
    }

    private Config getCurrentConfiguration(List<ConfigEntry> additional) {
        List<ConfigEntry> entryList = new ArrayList<>();

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("current-kafka-broker.conf")) {

            List<String> configList = Arrays.asList(ReadWriteUtils.readInputStream(is).split(System.lineSeparator()));
            configList.forEach(entry -> {
                String[] split = entry.split("=");
                String val = split.length == 1 ? "" : split[1];
                entryList.add(instantiateConfigEntry(split[0].replace("\n", ""), val));
            });
            entryList.addAll(additional);
        } catch (IOException e) {
            fail(e);
        }

        return new Config(entryList);
    }

    private void assertConfig(KafkaConfigurationDiff kcd, ConfigEntry ce) {
        Collection<AlterConfigOp> brokerDiffConf = kcd.getConfigDiff();
        long appearances = brokerDiffConf.stream().filter(entry -> entry.configEntry().name().equals(ce.name())).count();
        Optional<AlterConfigOp> en = brokerDiffConf.stream().filter(entry -> entry.configEntry().name().equals(ce.name())).findFirst();
        assertThat(appearances, is(1L));
        assertThat(en.isPresent(), is(true));
        assertThat(en.get().configEntry().name(), is(ce.name()));
        assertThat(en.get().configEntry().value(), is(ce.value()));
    }

    @Test
    public void testCustomPropertyAdded() {
        ArrayList<ConfigEntry> ces = new ArrayList<>();
        ces.add(new ConfigEntry("custom.property", "42"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(new ArrayList<>()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));

    }

    @Test
    public void testCustomPropertyRemoved() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        // custom changes are applied by changing STS
    }

    @Test
    public void testCustomPropertyKept() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testCustomPropertyChanged() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42"));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("custom.property", "43"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedPresentValue() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "1"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        assertConfig(kcd, new ConfigEntry("min.insync.replicas", "1"));
    }

    @Test
    public void testChangedPresentValueToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "2"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedControllerConfigForBrokerNode() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "5000"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "1000"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
    }

    @Test
    public void testChangedControllerConfigForCombinedNode() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "5000"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "1000"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, combinedNodeRef, true, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedBrokerConfigForCombinedNode() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("log.retention.hours", "72"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("log.retention.hours", "168"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, combinedNodeRef, true, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedControllerConfigForControllerNode() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "5000"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "1000"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, controllerNodeRef, true, false);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedControllerDynamicConfig() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("max.connections", "1000"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, controllerNodeRef, true, false);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedBrokerConfigForControllerNode() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("log.retention.hours", "72"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("log.retention.hours", "168"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, controllerNodeRef, true, false);
        assertThat(kcd.getDiffSize(), is(0));
    }


    @Test
    public void testChangedAdvertisedListenerForBroker() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "karel"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerForController() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "karel"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, controllerNodeRef, true, false);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerForCombined() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "karel"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, combinedNodeRef, true, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }


    @Test
    public void testChangedAdvertisedListenerFromNothingToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerFromNonDefaultToDefault() {
        // advertised listeners are filled after the pod started
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
        assertConfig(kcd, new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel"));
    }

    @Test
    public void testLogDirsNonDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
        assertConfig(kcd, new ConfigEntry("log.dirs", "null"));
    }

    @Test
    public void testLogDirsDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testUnchangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);

        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedInterBrokerListenerName() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("inter.broker.listener.name", "david"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedListenerSecurityProtocolMap() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "david"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedListenerSecurityProtocolMapFromNonDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL,EXTERNAL-9094:SSL"));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        assertConfig(kcd, new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL"));
    }

    @Test
    public void testChangedMoreProperties() {
        ArrayList<ConfigEntry> ces = new ArrayList<>(3);
        // change 3 random properties to observe whether diff has 3 entries
        ces.add(new ConfigEntry("inter.broker.listener.name", "david"));
        ces.add(new ConfigEntry("group.min.session.timeout.ms", "42"));
        ces.add(new ConfigEntry("auto.create.topics.enable", "false"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(3));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testRemoveDefaultPropertyWhichIsNotDefault() {
        // it is not seen as default because the ConfigEntry.ConfigSource.DEFAULT_CONFIG is not set
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.retention.hours", "168"));
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testClusterWideChanged() {
        List<ConfigEntry> current = List.of(
                new ConfigEntry("min.insync.replicas", "1"),
                new ConfigEntry("log.retention.bytes", "2000000")
        );
        List<ConfigEntry> desired = List.of(
                new ConfigEntry("min.insync.replicas", "2"),
                new ConfigEntry("log.retention.bytes", "3000000")
        );
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(current),
                getDesiredConfiguration(desired), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(2));
        assertThat(kcd.getConfigDiff(Scope.CLUSTER_WIDE).size(), is(2));
        assertThat(kcd.getConfigDiff(Scope.PER_BROKER).size(), is(0));
    }

    @Test
    public void testPropertiesAllScopesChanged() {
        List<ConfigEntry> current = List.of(
                new ConfigEntry("min.insync.replicas", "1"),
                new ConfigEntry("log.retention.bytes", "2000000"),
                new ConfigEntry("listener.security.protocol.map", "foo"),
                new ConfigEntry("auto.create.topics.enable", "true")
        );
        List<ConfigEntry> desired = List.of(
                new ConfigEntry("min.insync.replicas", "2"),
                new ConfigEntry("log.retention.bytes", "3000000"),
                new ConfigEntry("listener.security.protocol.map", "bar"),
                new ConfigEntry("auto.create.topics.enable", "false")
        );
        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(current),
                getDesiredConfiguration(desired), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(4));
        assertThat(kcd.getConfigDiff(Scope.CLUSTER_WIDE).size(), is(2));
        assertThat(kcd.getConfigDiff(Scope.PER_BROKER).size(), is(1));
        assertThat(kcd.getConfigDiff(Scope.READ_ONLY).size(), is(1));
    }

    @Test
    public void testAreDoublesEqual() {
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of(), Map.of("test.option", "0.8")), is(false));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "0.8"), Map.of()), is(false));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "0.9"), Map.of("test.option", "0.8")), is(false));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "0.8"), Map.of("test.option", "0.8")), is(true));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "1.024E8"), Map.of("test.option", "102400000")), is(true));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "0.8"), Map.of("test.option", "8e-1")), is(true));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "0.08"), Map.of("test.option", "8e-2")), is(true));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "1.797e+35"), Map.of("test.option", "179700000000000000000000000000000000")), is(true));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "1.7976931348623157E308"), Map.of("test.option", "1797.6931348623157E305")), is(true));
        assertThat(KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "1.7976931348623157E308"), Map.of("test.option", "1.7976931348623157E308")), is(true));
    }

    @Test
    public void testAreDoublesEqualFailedConversion() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> KafkaConfigurationDiff.areDoublesEqual("test.option", Map.of("test.option", "0.5"), Map.of("test.option", "0,5")));
        assertThat(e.getMessage(), is("Cannot compare double property 'test.option'"));
        assertThat(e.getCause().getClass(), is(NumberFormatException.class));
        assertThat(e.getCause().getMessage(), is("For input string: \"0,5\""));
    }

    @Test
    public void testDoublePropertiesAreIgnoredWhenEqual() {
        List<ConfigEntry> current = List.of(
                new ConfigEntry("log.cleaner.io.buffer.load.factor", "0.8"),
                new ConfigEntry("log.cleaner.io.max.bytes.per.second", "1.024E8"),
                new ConfigEntry("log.cleaner.min.cleanable.ratio", "0.6"),
                new ConfigEntry("sasl.login.refresh.window.jitter", "0.06")
        );
        List<ConfigEntry> desired = List.of(
                new ConfigEntry("log.cleaner.io.buffer.load.factor", "8e-1"),
                new ConfigEntry("log.cleaner.io.max.bytes.per.second", "102400000"),
                new ConfigEntry("log.cleaner.min.cleanable.ratio", "0.6"),
                new ConfigEntry("sasl.login.refresh.window.jitter", "0.06")
        );

        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(current), getDesiredConfiguration(desired), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(0));
    }

    @Test
    public void testDoublePropertiesAreUpdatedWhenNotEqual() {
        List<ConfigEntry> current = List.of(
                new ConfigEntry("log.cleaner.io.buffer.load.factor", "0.8"),
                new ConfigEntry("log.cleaner.io.max.bytes.per.second", "1.024E8")
        );
        List<ConfigEntry> desired = List.of(
                new ConfigEntry("log.cleaner.io.buffer.load.factor", "8e-2"),
                new ConfigEntry("log.cleaner.io.max.bytes.per.second", "10240000")
        );

        KafkaConfigurationDiff kcd = new KafkaConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(current), getDesiredConfiguration(desired), kafkaVersion, brokerNodeRef, false, true);
        assertThat(kcd.getDiffSize(), is(2));
        assertThat(kcd.getConfigDiff(Scope.CLUSTER_WIDE).size(), is(2));
        assertThat(kcd.getConfigDiff(Scope.CLUSTER_WIDE), hasItem(new AlterConfigOp(new ConfigEntry("log.cleaner.io.buffer.load.factor", "8e-2"), AlterConfigOp.OpType.SET)));
        assertThat(kcd.getConfigDiff(Scope.CLUSTER_WIDE), hasItem(new AlterConfigOp(new ConfigEntry("log.cleaner.io.max.bytes.per.second", "10240000"), AlterConfigOp.OpType.SET)));
    }
}

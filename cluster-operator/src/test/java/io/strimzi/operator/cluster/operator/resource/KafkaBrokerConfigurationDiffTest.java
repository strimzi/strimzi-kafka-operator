/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
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
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaBrokerConfigurationDiffTest {
    KafkaVersion kafkaVersion = KafkaVersionTestUtils.getKafkaVersionLookup().defaultVersion();
    private final NodeRef nodeRef = new NodeRef("broker-0", 0, "broker", false, true);

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
            String desiredConfigString = TestUtils.readResource(is);

            for (ConfigEntry ce : additional) {
                desiredConfigString += "\n" + ce.name() + "=" + ce.value();
            }

            return desiredConfigString;
        } catch (IOException e) {
            fail(e);
        }
        return "";
    }

    private Config getCurrentConfiguration(List<ConfigEntry> additional) {
        List<ConfigEntry> entryList = new ArrayList<>();

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("current-kafka-broker.conf")) {

            List<String> configList = Arrays.asList(TestUtils.readResource(is).split(System.getProperty("line.separator")));
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

    private void assertConfig(KafkaBrokerConfigurationDiff kcd, ConfigEntry ce) {
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
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(new ArrayList<>()), getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));

    }

    @Test
    public void testCustomPropertyRemoved() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        // custom changes are applied by changing STS
    }

    @Test
    public void testCustomPropertyKept() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testCustomPropertyChanged() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42"));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("custom.property", "43"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedPresentValue() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "2"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        assertConfig(kcd, new ConfigEntry("min.insync.replicas", "2"));
    }

    @Test
    public void testChangedPresentValueToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "1"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedKRaftControllerConfig() {
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "5000"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "1000"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
    }

    @Test
    public void testChangedKRaftControllerConfigForCombinedNode() {
        NodeRef combinedNodeId = new NodeRef("broker-0", 0, "broker", true, true);
        List<ConfigEntry> desiredControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "5000"));
        List<ConfigEntry> currentControllerConfig = singletonList(new ConfigEntry("controller.quorum.election.timeout.ms", "1000"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(currentControllerConfig),
                getDesiredConfiguration(desiredControllerConfig), kafkaVersion, combinedNodeId);
        assertThat(kcd.getDiffSize(), is(1));
    }

    @Test
    public void testChangedAdvertisedListener() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "karel"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerFromNothingToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerFromNonDefaultToDefault() {
        // advertised listeners are filled after the pod started
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedZookeeperConnect() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("zookeeper.connect", "karel"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
        assertConfig(kcd, new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel"));
    }

    @Test
    public void testLogDirsNonDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
        assertConfig(kcd, new ConfigEntry("log.dirs", "null"));
    }

    @Test
    public void testLogDirsDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testUnchangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);

        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedInterBrokerListenerName() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("inter.broker.listener.name", "david"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedListenerSecurityProtocolMap() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "david"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedListenerSecurityProtocolMapFromNonDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL,EXTERNAL-9094:SSL"));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, nodeRef);
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
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(3));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testRemoveDefaultPropertyWhichIsNotDefault() {
        // it is not seen as default because the ConfigEntry.ConfigSource.DEFAULT_CONFIG is not set
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.retention.hours", "168"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, nodeRef);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaBrokerConfigurationDiffTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final String KAFKA_VERSION = "2.7.0";
    KafkaVersion kafkaVersion = VERSIONS.version(KAFKA_VERSION);
    private int brokerId = 0;

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
                ConfigEntry ce = new ConfigEntry(split[0].replace("\n", ""), val, true, true, false);
                entryList.add(ce);
            });
            for (ConfigEntry ce : additional) {
                entryList.add(ce);
            }
        } catch (IOException e) {
            fail(e);
        }

        Config config = new Config(entryList);
        return config;
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
    public void testDefaultValue() {
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()), getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.isDesiredPropertyDefaultValue("offset.metadata.max.bytes", getCurrentConfiguration(emptyList())), is(true));
    }

    @Test
    public void testNonDefaultValue() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("offset.metadata.max.bytes", "4097"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.isDesiredPropertyDefaultValue("offset.metadata.max.bytes", getCurrentConfiguration(ces)), is(false));
    }

    @Test
    public void testCustomPropertyAdded() {
        ArrayList<ConfigEntry> ces = new ArrayList<>();
        ces.add(new ConfigEntry("custom.property", "42"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(new ArrayList<>()), getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));

    }

    @Test
    public void testCustomPropertyRemoved() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        // custom changes are applied by changing STS
    }

    @Test
    public void testCustomPropertyKept() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testCustomPropertyChanged() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42", false, true, false));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("custom.property", "43", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedPresentValue() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "2", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        assertConfig(kcd, new ConfigEntry("min.insync.replicas", "2"));
    }

    @Test
    public void testChangedPresentValueToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "1", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListener() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "karel", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerFromNothingToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedAdvertisedListenerFromNonDefaultToDefault() {
        // advertised listeners are filled after the pod started
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedZookeeperConnect() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("zookeeper.connect", "karel", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
        assertConfig(kcd, new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel"));
    }

    @Test
    public void testLogDirsNonDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
        assertConfig(kcd, new ConfigEntry("log.dirs", "null"));
    }

    @Test
    public void testLogDirsDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testUnchangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(0));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedInterBrokerListenerName() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("inter.broker.listener.name", "david", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedListenerSecurityProtocolMap() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "david", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedListenerSecurityProtocolMapFromNonDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL,EXTERNAL-9094:SSL", false, true, false));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(true));
        assertConfig(kcd, new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL"));
    }

    @Test
    public void testChangedMoreProperties() {
        ArrayList<ConfigEntry> ces = new ArrayList<>();
        ces.add(new ConfigEntry("inter.broker.listener.name", "david", false, true, false));
        ces.add(new ConfigEntry("group.min.session.timeout.ms", "42", false, true, false));
        ces.add(new ConfigEntry("host.name", "honza", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(3));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testRemoveDefaultPropertyWhichIsNotDefault() {
        // it is not seen as default because the ConfigEntry.ConfigSource.DEFAULT_CONFIG is not set
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.retention.hours", "168"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.getDiffSize(), is(1));
        assertThat(kcd.canBeUpdatedDynamically(), is(false));
    }

}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaBrokerLoggingConfigurationDiffTest {

    private final int brokerId = 0;

    private String getDesiredConfiguration(List<ConfigEntry> additional) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("desired-kafka-broker-logging.conf")) {
            StringBuilder desiredConfigString = new StringBuilder(TestUtils.readResource(is));

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

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("current-kafka-broker-logging.conf")) {

            List<String> configList = Arrays.asList(TestUtils.readResource(is).split(System.getProperty("line.separator")));
            configList.forEach(entry -> {
                String[] split = entry.split("=");
                String val = split.length == 1 ? "" : split[1];
                ConfigEntry ce = new ConfigEntry(split[0].replace("\n", ""), val, true, true, false);
                entryList.add(ce);
            });
            entryList.addAll(additional);
        } catch (IOException e) {
            fail(e);
        }

        return new Config(entryList);
    }

    @Test
    public void testReplaceRootLogger() {
        KafkaBrokerLoggingConfigurationDiff klcd = new KafkaBrokerLoggingConfigurationDiff(getCurrentConfiguration(emptyList()), getDesiredConfiguration(emptyList()), brokerId);
        assertThat(klcd.getDiffSize(), is(0));
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaBrokerLoggingConfigurationDiffTest {

    private final int brokerId = 0;

    private String getDesiredConfiguration(List<ConfigEntry> additional) {
        StringBuilder desiredConfigString = new StringBuilder();
        desiredConfigString.append("# Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.\n" +
                "log4j.rootLogger=INFO, CONSOLE");

        for (ConfigEntry ce : additional) {
            desiredConfigString.append("\n").append(ce.name()).append("=").append(ce.value());
        }
        return desiredConfigString.toString();
    }

    private Config getCurrentConfiguration(List<ConfigEntry> additional) {
        List<ConfigEntry> entryList = new ArrayList<>();
        String current = "root=INFO";

        List<String> configList = Arrays.asList(current.split(System.getProperty("line.separator")));
        configList.forEach(entry -> {
            String[] split = entry.split("=");
            String val = split.length == 1 ? "" : split[1];
            ConfigEntry ce = new ConfigEntry(split[0].replace("\n", ""), val, true, true, false);
            entryList.add(ce);
        });
        entryList.addAll(additional);

        return new Config(entryList);
    }

    @Test
    public void testReplaceRootLogger() {
        KafkaBrokerLoggingConfigurationDiff klcd = new KafkaBrokerLoggingConfigurationDiff(getCurrentConfiguration(emptyList()), getDesiredConfiguration(emptyList()), brokerId);
        assertThat(klcd.getDiffSize(), is(0));
    }
}

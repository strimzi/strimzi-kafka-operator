/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaBrokerLoggingConfigurationDiffTest {

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
            ConfigEntry ce = new ConfigEntry(split[0].replace("\n", ""), val);
            entryList.add(ce);
        });
        entryList.addAll(additional);

        return new Config(entryList);
    }

    @Test
    public void testReplaceRootLogger() {
        KafkaBrokerLoggingConfigurationDiff klcd = new KafkaBrokerLoggingConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, getCurrentConfiguration(emptyList()), getDesiredConfiguration(emptyList()));
        assertThat(klcd.getDiffSize(), is(0));
    }

    @Test
    public void testDiffUsingLoggerInheritance() {
        // Prepare desiredConfig
        String desiredConfig = getRealisticDesiredConfig();

        // Prepare currentConfig
        Config currentConfig = getRealisticConfig();

        KafkaBrokerLoggingConfigurationDiff diff = new KafkaBrokerLoggingConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, currentConfig, desiredConfig);
        assertThat(diff.getLoggingDiff(), is(getRealisticConfigDiff()));
    }

    Config getRealisticConfig() {
        return new Config(Arrays.asList(
            new ConfigEntry("org.apache.zookeeper.CreateMode", "INFO"),
            new ConfigEntry("io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler", "INFO"),
            new ConfigEntry("io.netty.util.concurrent.PromiseNotifier", "INFO"),
            new ConfigEntry("kafka.server.DynamicConfigManager", "INFO"),
            new ConfigEntry("org.apache.kafka.common.utils.AppInfoParser", "INFO"),
            new ConfigEntry("kafka.utils.KafkaScheduler", "INFO"),
            new ConfigEntry("io.netty.util.internal.PlatformDependent0", "INFO"),
            new ConfigEntry("org.apache.kafka.clients.ClientUtils", "INFO"),
            new ConfigEntry("org.apache.kafka", "DEBUG"),
            new ConfigEntry("state.change.logger", "TRACE"),
            new ConfigEntry("org.apache.kafka.common.security.authenticator.LoginManager", "INFO"),
            new ConfigEntry("io.strimzi.kafka.oauth.common.HttpUtil", "INFO"),
            new ConfigEntry("org.apache.zookeeper.ZooKeeper", "INFO"),
            new ConfigEntry("io.netty.util.internal.CleanerJava9", "INFO"),
            new ConfigEntry("org.apache.zookeeper.ClientCnxnSocket", "INFO"),
            new ConfigEntry("io.strimzi", "TRACE"),
            new ConfigEntry("kafka", "DEBUG"),
            new ConfigEntry("org.apache.zookeeper", "INFO"),
            new ConfigEntry("root", "INFO")));
    }

    String getRealisticDesiredConfig() {
        return "# Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.\n" +
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                "root.logger.level=INFO\n " +
                "log4j.rootLogger=${root.logger.level}, CONSOLE\n" +
                "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
                "log4j.logger.org.apache.zookeeper=INFO\n" +
                "log4j.logger.kafka=DEBUG\n" +
                "log4j.logger.org.apache.kafka=DEBUG\n" +
                "log4j.logger.kafka.request.logger=WARN, CONSOLE\n" +
                "log4j.logger.kafka.network.Processor=OFF\n" +
                "log4j.logger.kafka.server.KafkaApis=OFF\n" +
                "log4j.logger.kafka.network.RequestChannel$=WARN\n" +
                "log4j.logger.kafka.controller=TRACE\n" +
                "log4j.logger.kafka.log.LogCleaner=INFO\n" +
                "log4j.logger.state.change.logger=TRACE\n" +
                "log4j.logger.kafka.authorizer.logger=INFO\n" +
                "log4j.logger.io.strimzi=TRACE\n" +
                "log4j.logger.some.category=ALL\n" +
                "log4j.logger.another.category=FINE\n" +
                "log4j.logger.another.category.sub=TRACE";
    }

    List<AlterConfigOp> getRealisticConfigDiff() {
        return Arrays.asList(
            newAlterConfigOp("io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler", "TRACE"),
            newAlterConfigOp("kafka.server.DynamicConfigManager", "DEBUG"),
            newAlterConfigOp("org.apache.kafka.common.utils.AppInfoParser", "DEBUG"),
            newAlterConfigOp("kafka.utils.KafkaScheduler", "DEBUG"),
            newAlterConfigOp("org.apache.kafka.clients.ClientUtils", "DEBUG"),
            newAlterConfigOp("org.apache.kafka.common.security.authenticator.LoginManager", "DEBUG"),
            newAlterConfigOp("io.strimzi.kafka.oauth.common.HttpUtil", "TRACE"),
            newAlterConfigOp("org.I0Itec.zkclient.ZkClient", "INFO"),
            newAlterConfigOp("kafka.request.logger", "WARN"),
            newAlterConfigOp("kafka.network.Processor", "FATAL"),
            newAlterConfigOp("kafka.server.KafkaApis", "FATAL"),
            newAlterConfigOp("kafka.network.RequestChannel$", "WARN"),
            newAlterConfigOp("kafka.controller", "TRACE"),
            newAlterConfigOp("kafka.log.LogCleaner", "INFO"),
            newAlterConfigOp("kafka.authorizer.logger", "INFO"),
            newAlterConfigOp("some.category", "TRACE"),
            newAlterConfigOp("another.category", "WARN"),
            newAlterConfigOp("another.category.sub", "TRACE"));
    }

    AlterConfigOp newAlterConfigOp(String category, String level) {
        return new AlterConfigOp(new ConfigEntry(category, level), AlterConfigOp.OpType.SET);
    }

    @Test
    public void testExpansion() {
        String input = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                "log4j.rootLogger=${kafka.root.logger.level}, CONSOLE\n" +
                "\n" +
                "# Log levels\n" +
                // level definition is afterwards its use
                "kafka.root.logger.level=INFO\n" +
                "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
                "log4j.logger.org.apache.zookeeper=INFO\n" +
                "log4j.logger.kafka=INFO\n" +
                "log4j.logger.org.apache.kafka=INFO\n" +
                "log4j.logger.kafka.request.logger=WARN, CONSOLE\n" +
                "log4j.logger.kafka.network.Processor=OFF\n" +
                "log4j.logger.kafka.server.KafkaApis=OFF\n" +
                "log4j.logger.kafka.network.RequestChannel$=WARN\n" +
                "log4j.logger.kafka.controller=TRACE\n" +
                "log4j.logger.kafka.log.LogCleaner=INFO\n" +
                "log4j.logger.state.change.logger=TRACE\n" +
                "log4j.logger.kafka.authorizer.logger=INFO\n" +
                "monitorInterval=30\n";

        KafkaBrokerLoggingConfigurationDiff kdiff = new KafkaBrokerLoggingConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION, null, null);

        Map<String, String> res = kdiff.readLog4jConfig(input);
        assertThat(res.get("root"), is("INFO"));
        assertThat(res.get("kafka.request.logger"), is("WARN"));
    }
}

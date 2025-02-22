/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.logging;

import io.strimzi.api.kafka.model.common.InlineLoggingBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LoggingModelTest {
    @Test
    public void testLog4j1()   {
        LoggingModel model = new LoggingModel(
                new KafkaConnectSpecBuilder()
                        .withLogging(new InlineLoggingBuilder().withLoggers(Map.of("log4j.logger.org.reflections", "DEBUG", "logger.myclass.level", "TRACE")).build())
                        .build(),
                "KafkaConnectCluster",
                false,
                true
        );

        assertThat(model.configMapKey(), is("log4j.properties"));
        assertThat(model.getDefaultLogConfigBaseName(), is("KafkaConnectCluster.log4j1"));
        assertThat(model.isLog4j2(), is(false));
        assertThat(model.isShouldPatchLoggerAppender(), is(true));
        assertThat(model.loggingConfiguration(Reconciliation.DUMMY_RECONCILIATION, null), is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n
                connect.root.logger.level=INFO
                log4j.rootLogger=${connect.root.logger.level}, CONSOLE
                log4j.logger.org.reflections=DEBUG
                logger.myclass.level=TRACE
                """));
    }

    @Test
    public void testLog4j2()   {
        LoggingModel model = new LoggingModel(
                new KafkaConnectSpecBuilder().build(),
                "KafkaConnectCluster",
                true,
                false
        );

        assertThat(model.configMapKey(), is("log4j2.properties"));
        assertThat(model.getDefaultLogConfigBaseName(), is("KafkaConnectCluster"));
        assertThat(model.isLog4j2(), is(true));
        assertThat(model.isShouldPatchLoggerAppender(), is(false));
        assertThat(model.loggingConfiguration(Reconciliation.DUMMY_RECONCILIATION, null), is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                name=KafkaConnectConfig
                appender.console.type=Console
                appender.console.name=STDOUT
                appender.console.layout.type=PatternLayout
                appender.console.layout.pattern=%d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                rootLogger.level=INFO
                rootLogger.appenderRefs=console
                rootLogger.appenderRef.console.ref=STDOUT
                rootLogger.additivity=false
                logger.reflections.name=org.reflections
                logger.reflections.level=ERROR
                logger.reflections.additivity=false
                
                monitorInterval=30
                """));
    }
}

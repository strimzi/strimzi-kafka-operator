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
        assertThat(model.getDefaultLogConfigBaseName(), is("KafkaConnectCluster"));
        assertThat(model.isLog4j2(), is(false));
        assertThat(model.isShouldPatchLoggerAppender(), is(true));
        assertThat(model.loggingConfiguration(Reconciliation.DUMMY_RECONCILIATION, null), is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n
                connect.root.logger.level=INFO
                log4j.rootLogger=${connect.root.logger.level}, CONSOLE
                log4j.logger.org.apache.zookeeper=ERROR
                log4j.logger.org.I0Itec.zkclient=ERROR
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
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n
                connect.root.logger.level=INFO
                log4j.rootLogger=${connect.root.logger.level}, CONSOLE
                log4j.logger.org.apache.zookeeper=ERROR
                log4j.logger.org.I0Itec.zkclient=ERROR
                log4j.logger.org.reflections=ERROR
                                
                monitorInterval=30
                """));
    }
}

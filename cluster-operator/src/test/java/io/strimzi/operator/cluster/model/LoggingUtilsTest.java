/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.InlineLoggingBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.OrderedProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LoggingUtilsTest {
    @Test
    public void testDefaultLogConfigWithNonExistentFile()   {
        OrderedProperties logging = LoggingUtils.defaultLogConfig(Reconciliation.DUMMY_RECONCILIATION, "NonExistingClass");
        assertThat(logging.asMap(), is(Map.of()));
    }

    @Test
    public void testDefaultLogConfig()   {
        OrderedProperties logging = LoggingUtils.defaultLogConfig(Reconciliation.DUMMY_RECONCILIATION, "KafkaConnectCluster");

        assertThat(logging.asPairs(), is("""
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n
                connect.root.logger.level=INFO
                log4j.rootLogger=${connect.root.logger.level}, CONSOLE
                log4j.logger.org.apache.zookeeper=ERROR
                log4j.logger.org.I0Itec.zkclient=ERROR
                log4j.logger.org.reflections=ERROR
                """));
    }

    @Test
    public void testCreateLog4jProperties()   {
        OrderedProperties logging = new OrderedProperties();
        logging.addPair("my-key1", "my-value1");
        logging.addPair("my-key2", "my-value2");

        // Log4j1 does not have monitorInterval
        assertThat(LoggingUtils.createLog4jProperties(logging, false), is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                my-key1=my-value1
                my-key2=my-value2
                """));

        // Log4j2 does have monitorInterval
        assertThat(LoggingUtils.createLog4jProperties(logging, true), is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                my-key1=my-value1
                my-key2=my-value2
                
                monitorInterval=30
                """));

        logging.addPair("monitorInterval", "13");

        // Specified monitorInterval is not overwritten
        assertThat(LoggingUtils.createLog4jProperties(logging, true), is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                my-key1=my-value1
                my-key2=my-value2
                monitorInterval=13
                """));
    }

    @Test
    public void testNullLog4j1LoggingConfiguration()  {
        String log4jProperties = LoggingUtils.loggingConfiguration(
                Reconciliation.DUMMY_RECONCILIATION,
                "KafkaConnectCluster",
                true,
                false,
                null,
                null
        );

        assertThat(log4jProperties, is("""
                # Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n
                connect.root.logger.level=INFO
                log4j.rootLogger=${connect.root.logger.level}, CONSOLE
                log4j.logger.org.apache.zookeeper=ERROR
                log4j.logger.org.I0Itec.zkclient=ERROR
                log4j.logger.org.reflections=ERROR
                """));
    }

    @Test
    public void testNullLog4j2LoggingConfiguration()  {
        String log4jProperties = LoggingUtils.loggingConfiguration(
                Reconciliation.DUMMY_RECONCILIATION,
                "KafkaConnectCluster",
                true,
                true,
                null,
                null
        );

        assertThat(log4jProperties, is("""
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

    @Test
    public void testLog4j1InlineLoggingConfiguration()  {
        String log4jProperties = LoggingUtils.loggingConfiguration(
                Reconciliation.DUMMY_RECONCILIATION,
                "KafkaConnectCluster",
                true,
                false,
                new InlineLoggingBuilder().withLoggers(Map.of("log4j.logger.org.reflections", "DEBUG", "logger.myclass.level", "TRACE")).build(),
                null
        );

        assertThat(log4jProperties, is("""
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
    public void testLog4j2InlineLoggingConfiguration()  {
        String log4jProperties = LoggingUtils.loggingConfiguration(
                Reconciliation.DUMMY_RECONCILIATION,
                "KafkaConnectCluster",
                true,
                true,
                new InlineLoggingBuilder().withLoggers(Map.of("log4j.logger.org.reflections", "DEBUG", "logger.myclass.level", "TRACE")).build(),
                null
        );

        assertThat(log4jProperties, is("""
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
                
                monitorInterval=30
                """));
    }

    @Test
    public void testLog4j1ExternalLoggingConfiguration()  {
        String log4jProperties = LoggingUtils.loggingConfiguration(
                Reconciliation.DUMMY_RECONCILIATION,
                "KafkaConnectCluster",
                true,
                false,
                new ExternalLoggingBuilder()
                        .withNewValueFrom()
                            .withNewConfigMapKeyRef("my-key", "my-cm", false)
                        .endValueFrom()
                        .build(),
                new ConfigMapBuilder()
                        .withData(Map.of("my-key", """
                                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n
                                zookeeper.root.logger=INFO
                                log4j.rootLogger=${zookeeper.root.logger}, CONSOLE
                                """))
                        .build()
        );

        assertThat(log4jProperties, is("""
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n
                zookeeper.root.logger=INFO
                log4j.rootLogger=${zookeeper.root.logger}, CONSOLE
                """));
    }

    @Test
    public void testLog4j2ExternalLoggingConfiguration()  {
        String log4jProperties = LoggingUtils.loggingConfiguration(
                Reconciliation.DUMMY_RECONCILIATION,
                "KafkaConnectCluster",
                true,
                true,
                new ExternalLoggingBuilder()
                        .withNewValueFrom()
                        .withNewConfigMapKeyRef("my-key", "my-cm", false)
                        .endValueFrom()
                        .build(),
                new ConfigMapBuilder()
                        .withData(Map.of("my-key", """
                                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n
                                zookeeper.root.logger=INFO
                                log4j.rootLogger=${zookeeper.root.logger}, CONSOLE
                                """))
                        .build()
        );

        assertThat(log4jProperties, is("""
                log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
                log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
                log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n
                zookeeper.root.logger=INFO
                log4j.rootLogger=${zookeeper.root.logger}, CONSOLE
                                
                monitorInterval=30
                """));
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.logging;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.strimzi.api.kafka.model.common.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.common.InlineLoggingBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.OrderedProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
                new LoggingModel(new KafkaConnectSpec(), "KafkaConnectCluster", false, true),
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
                new LoggingModel(new KafkaConnectSpec(), "KafkaConnectCluster", true, true),
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
                new LoggingModel(
                        new KafkaConnectSpecBuilder()
                                .withLogging(new InlineLoggingBuilder().withLoggers(Map.of("log4j.logger.org.reflections", "DEBUG", "logger.myclass.level", "TRACE")).build())
                                .build(),
                        "KafkaConnectCluster",
                        false,
                        true),
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
                new LoggingModel(
                        new KafkaConnectSpecBuilder()
                                .withLogging(new InlineLoggingBuilder().withLoggers(Map.of("log4j.logger.org.reflections", "DEBUG", "logger.myclass.level", "TRACE")).build())
                                .build(),
                        "KafkaConnectCluster",
                        true,
                        true),
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
                new LoggingModel(
                        new KafkaConnectSpecBuilder()
                                .withLogging(new ExternalLoggingBuilder()
                                        .withNewValueFrom()
                                        .withNewConfigMapKeyRef("my-key", "my-cm", false)
                                        .endValueFrom()
                                        .build())
                                .build(),
                        "KafkaConnectCluster",
                        false,
                        true),
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
                new LoggingModel(
                        new KafkaConnectSpecBuilder()
                                .withLogging(new ExternalLoggingBuilder()
                                        .withNewValueFrom()
                                        .withNewConfigMapKeyRef("my-key", "my-cm", false)
                                        .endValueFrom()
                                        .build())
                                .build(),
                        "KafkaConnectCluster",
                        true,
                        true),
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

    @Test
    public void testLoggingValidation() {
        assertDoesNotThrow(() -> LoggingUtils.validateLogging(null));

        // Inline logging
        assertDoesNotThrow(() -> LoggingUtils.validateLogging(new InlineLoggingBuilder().build()));
        assertDoesNotThrow(() -> LoggingUtils.validateLogging(new InlineLoggingBuilder().withLoggers(Map.of("my.logger", "WARN")).build()));

        // ExternalLogging
        assertDoesNotThrow(() -> LoggingUtils.validateLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("my-key", "my-name", false)).endValueFrom().build()));

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> LoggingUtils.validateLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector()).endValueFrom().build()));
        assertThat(ex.getMessage(), is("Logging configuration is invalid: [Name of the Config Map with logging configuration is missing, The key under which the logging configuration is stored in the ConfigMap is missing]"));

        ex = assertThrows(InvalidResourceException.class, () -> LoggingUtils.validateLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector(null, "my-name", false)).endValueFrom().build()));
        assertThat(ex.getMessage(), is("Logging configuration is invalid: [The key under which the logging configuration is stored in the ConfigMap is missing]"));

        ex = assertThrows(InvalidResourceException.class, () -> LoggingUtils.validateLogging(new ExternalLoggingBuilder().withNewValueFrom().endValueFrom().build()));
        assertThat(ex.getMessage(), is("Logging configuration is invalid: [Config Map reference is missing]"));

        ex = assertThrows(InvalidResourceException.class, () -> LoggingUtils.validateLogging(new ExternalLoggingBuilder().build()));
        assertThat(ex.getMessage(), is("Logging configuration is invalid: [Config Map reference is missing]"));
    }
}

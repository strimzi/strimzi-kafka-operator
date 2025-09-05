/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.logging;

import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LoggingModelTest {

    @Test
    public void testLog4j2()   {
        LoggingModel model = new LoggingModel(
                new KafkaConnectSpecBuilder().build(),
                "KafkaConnectCluster"
        );

        assertThat(LoggingModel.LOG4J2_CONFIG_MAP_KEY, is("log4j2.properties"));
        assertThat(model.getDefaultLogConfigBaseName(), is("KafkaConnectCluster"));
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

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.logging.TestLogger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ValidationVisitorTest {
    @Test
    public void testValidationErrorsAreLogged() {
        Kafka k = ReadWriteUtils.readObjectFromYamlFileInResources("/example.yaml", Kafka.class, true);
        assertThat(k, is(notNullValue()));
        TestLogger logger = TestLogger.create(ValidationVisitorTest.class);

        Set<Condition> warningConditions = new HashSet<>();

        ResourceVisitor.visit(Reconciliation.DUMMY_RECONCILIATION, k, new ValidationVisitor(k, logger, warningConditions));

        List<String> warningMessages = warningConditions.stream().map(Condition::getMessage).collect(Collectors.toList());

        assertThat(warningMessages, hasItem("Resource Kafka(myproject/my-cluster) contains object at path spec.kafka with an unknown property: foo"));
        // TODO: These assertions for deprecation warnings (similar ones) should be restored once we have some deprecated fields again
        //assertThat(warningMessages, hasItem("In resource Kafka(testnamespace/testname) in API version kafka.strimzi.io/v1 the enableECDSA property at path spec.kafka.listeners.auth.enableECDSA has been deprecated."));
        //assertThat(warningMessages, hasItem("In resource Kafka(testnamespace/testname) in API version kafka.strimzi.io/v1 the service property at path spec.kafkaExporter.template.service has been deprecated. " +
        //        "The Kafka Exporter service has been removed."));

        logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
                && lm.formattedMessage().matches("Reconciliation #[0-9]*\\(test\\) kind\\(namespace/name\\): " +
                "Resource Kafka\\(myproject/my-cluster\\) contains object at path spec.kafka with an unknown property: foo"));
        // TODO: These assertions for deprecation warnings (similar ones) should be restored once we have some deprecated fields again
        //logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
        //        && lm.formattedMessage().matches("Reconciliation #[0-9]*\\(test\\) kind\\(namespace/name\\): " +
        //        "In resource Kafka\\(testnamespace/testname\\) in API version kafka.strimzi.io/v1 the enableECDSA property at path spec.kafka.listeners.auth.enableECDSA has been deprecated."));
        //logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
        //        && lm.formattedMessage().matches("Reconciliation #[0-9]*\\(test\\) kind\\(namespace/name\\): " +
        //        "In resource Kafka\\(testnamespace/testname\\) in API version kafka.strimzi.io/v1 the service property at path spec.kafkaExporter.template.service has been deprecated. " +
        //        "The Kafka Exporter service has been removed."));
    }
}

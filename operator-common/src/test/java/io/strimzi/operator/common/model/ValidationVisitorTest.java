/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.test.TestUtils;
import io.strimzi.test.logging.TestLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ValidationVisitorTest {

    @Test
    public void testValidationErrorsAreLogged() {
        Kafka k = TestUtils.fromYaml("/example.yaml", Kafka.class, true);
        assertNotNull(k);
        TestLogger logger = new TestLogger((Logger) LogManager.getLogger(ValidationVisitorTest.class));
        HasMetadata resource = new KafkaBuilder()
                .withNewMetadata()
                    .withName("testname")
                    .withNamespace("testnamespace")
                .endMetadata()
                .withApiVersion("v1alpha1")
            .build();
        ResourceVisitor.visit(k, new ValidationVisitor(resource, logger));
        logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
            && ("Kafka resource testname in namespace testnamespace: " +
                "Contains object at path spec.kafka with an unknown property: foo").equals(lm.formattedMessage()));
        logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
                && ("Kafka resource testname in namespace testnamespace: " +
                "In API version v1alpha1 the property topicOperator at path spec.topicOperator has been deprecated. " +
                "This feature should now be configured at path spec.entityOerator.topicOperator.").equals(lm.formattedMessage()));
    }


}

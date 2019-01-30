/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.Kafka;
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
        ResourceVisitor.visit(k, new ValidationVisitor("test", logger));
        logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
            && "test contains object at path spec.kafka with an unknown property: foo".equals(lm.formattedMessage()));
        logger.assertLoggedAtLeastOnce(lm -> lm.level() == Level.WARN
                && ("test contains an object at path spec.topicOperator but the property " +
                "topicOperator is deprecated and will be removed in a future release").equals(lm.formattedMessage()));
    }


}

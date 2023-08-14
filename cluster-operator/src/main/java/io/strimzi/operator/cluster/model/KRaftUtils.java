/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.KafkaSpec;

import java.util.HashSet;
import java.util.Set;

/**
 * Shared methods for working with KRaft
 */
public class KRaftUtils {
    /**
     * In KRaft mode, multiple features are currently not supported. This method validates the Kafka CR for the
     * unsupported features and if they are used, throws an InvalidResourceException exception.
     *
     * @param kafkaSpec   The .spec section of the Kafka CR which should be checked
     * @param utoEnabled  Flag indicating whether Unidirectional Topic Operator is enabled or not
     */
    public static void validateKafkaCrForKRaft(KafkaSpec kafkaSpec, boolean utoEnabled)   {
        Set<String> errors = new HashSet<>(0);

        if (kafkaSpec != null)  {
            validateEntityOperatorSpec(errors, kafkaSpec.getEntityOperator(), utoEnabled);
        } else {
            errors.add("The .spec section of the Kafka custom resource is missing");
        }

        if (!errors.isEmpty())  {
            throw new InvalidResourceException("Kafka configuration is not valid: " + errors);
        }
    }

    /**
     * Checks whether the Topic Operator is configured or not
     *
     * @param errors            Set with detected errors to which any new errors should be added
     * @param entityOperator    The Entity Operator spec which should be checked
     * @param utoEnabled        Flag indicating whether Unidirectional Topic Operator is enabled or not
     */
    /* test */ static void validateEntityOperatorSpec(Set<String> errors, EntityOperatorSpec entityOperator, boolean utoEnabled) {
        if (entityOperator != null && entityOperator.getTopicOperator() != null && !utoEnabled) {
            errors.add("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled. You can enable it using the UnidirectionalTopicOperator feature gate.");
        }
    }
}

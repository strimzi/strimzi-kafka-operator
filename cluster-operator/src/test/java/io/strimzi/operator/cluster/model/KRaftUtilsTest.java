/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.KafkaSpecBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KRaftUtilsTest {
    @ParallelTest
    public void testValidKafka() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8080")
                    .endKafkaAuthorizationOpa()
                .endKafka()
                .build();

        assertDoesNotThrow(() -> KRaftUtils.validateKafkaCrForKRaft(spec, false));
    }

    @ParallelTest
    public void testInvalidKafka() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build())
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                                new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                    .endJbodStorage()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateKafkaCrForKRaft(spec, false));

        assertThat(ex.getMessage(), is("Kafka configuration is not valid: [Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled. You can enable it using the UnidirectionalTopicOperator feature gate.]"));
    }

    @ParallelTest
    public void testNoEntityOperator() {
        Set<String> errors = new HashSet<>(0);
        KRaftUtils.validateEntityOperatorSpec(errors, null, false);

        assertThat(errors, is(Collections.emptySet()));
    }

    @ParallelTest
    public void testEnabledOnlyUserOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, false);
        assertThat(errors, is(Collections.emptySet()));
    }

    @ParallelTest
    public void testEnabledEntityOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, false);

        assertThat(errors, is(Set.of("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled. You can enable it using the UnidirectionalTopicOperator feature gate.")));
    }

    @ParallelTest
    public void testEnabledUnidirectionalTopicOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, true);

        assertThat(errors.size(), is(0));
    }

    @ParallelTest
    public void testEnabledEntityOperatorOnlyTopicOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, false);

        assertThat(errors, is(Set.of("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled. You can enable it using the UnidirectionalTopicOperator feature gate.")));
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.api.kafka.model.kafka.KafkaSpecBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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

        assertThat(ex.getMessage(), is("Kafka configuration is not valid: [Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled.]"));
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

        assertThat(errors, is(Set.of("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled.")));
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

        assertThat(errors, is(Set.of("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled.")));
    }

    @ParallelTest
    public void testKRaftMetadataVersionValidation()    {
        // Valid values
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.6"));
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.6-IV2"));

        // Minimum supported versions
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.3"));
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.3-IV0"));

        // Invalid Values
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3.6-IV9"));
        assertThat(e.getMessage(), containsString("Metadata version 3.6-IV9 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3"));
        assertThat(e.getMessage(), containsString("Metadata version 3 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3.2"));
        assertThat(e.getMessage(), containsString("The oldest supported metadata version is 3.3-IV0"));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3.2-IV0"));
        assertThat(e.getMessage(), containsString("The oldest supported metadata version is 3.3-IV0"));
    }
}

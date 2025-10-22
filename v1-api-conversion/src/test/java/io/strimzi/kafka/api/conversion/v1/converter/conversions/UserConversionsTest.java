/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclRuleBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
class UserConversionsTest extends AbstractConversionsTest {
    @Test
    public void testOperationToOperations() {
        KafkaUser user = new KafkaUserBuilder()
                .withNewSpec()
                    .withNewKafkaUserAuthorizationSimple()
                        .withAcls(new AclRuleBuilder().withNewAclRuleTopicResource().withName("my-topic").endAclRuleTopicResource().withOperation(AclOperation.READ).build(),
                                new AclRuleBuilder().withNewAclRuleTopicResource().withName("my-topic2").endAclRuleTopicResource().withOperation(AclOperation.READ).withOperations(List.of(AclOperation.WRITE)).build(),
                                new AclRuleBuilder().withNewAclRuleTopicResource().withName("my-topic3").endAclRuleTopicResource().withOperations(List.of(AclOperation.DESCRIBE)).build())
                    .endKafkaUserAuthorizationSimple()
                .endSpec()
                .build();

        Conversion<KafkaUser> c = UserConversions.operationToOperations();

        c.convert(user);

        KafkaUserAuthorizationSimple authorization = (KafkaUserAuthorizationSimple) user.getSpec().getAuthorization();
        assertThat(authorization.getAcls().size(), is(3));
        assertThat(authorization.getAcls().get(0).getOperation(), is(nullValue()));
        assertThat(authorization.getAcls().get(0).getOperations(), is(List.of(AclOperation.READ)));
        assertThat(authorization.getAcls().get(1).getOperation(), is(nullValue()));
        assertThat(authorization.getAcls().get(1).getOperations(), is(List.of(AclOperation.WRITE)));
        assertThat(authorization.getAcls().get(2).getOperation(), is(nullValue()));
        assertThat(authorization.getAcls().get(2).getOperations(), is(List.of(AclOperation.DESCRIBE)));
    }

    @Test
    public void testOperationToOperationsJson() {
        KafkaUser user = new KafkaUserBuilder()
                .withNewSpec()
                    .withNewKafkaUserAuthorizationSimple()
                        .withAcls(new AclRuleBuilder().withNewAclRuleTopicResource().withName("my-topic").endAclRuleTopicResource().withOperation(AclOperation.READ).build(),
                                new AclRuleBuilder().withNewAclRuleTopicResource().withName("my-topic2").endAclRuleTopicResource().withOperation(AclOperation.READ).withOperations(List.of(AclOperation.WRITE)).build(),
                                new AclRuleBuilder().withNewAclRuleTopicResource().withName("my-topic3").endAclRuleTopicResource().withOperations(List.of(AclOperation.DESCRIBE)).build())
                    .endKafkaUserAuthorizationSimple()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(user);

        Conversion<KafkaUser> c = UserConversions.operationToOperations();
        c.convert(json);

        KafkaUserAuthorizationSimple authorization = (KafkaUserAuthorizationSimple) jsonNodeToTyped(json, KafkaUser.class).getSpec().getAuthorization();
        assertThat(authorization.getAcls().size(), is(3));
        assertThat(authorization.getAcls().get(0).getOperation(), is(nullValue()));
        assertThat(authorization.getAcls().get(0).getOperations(), is(List.of(AclOperation.READ)));
        assertThat(authorization.getAcls().get(1).getOperation(), is(nullValue()));
        assertThat(authorization.getAcls().get(1).getOperations(), is(List.of(AclOperation.WRITE)));
        assertThat(authorization.getAcls().get(2).getOperation(), is(nullValue()));
        assertThat(authorization.getAcls().get(2).getOperations(), is(List.of(AclOperation.DESCRIBE)));
    }
}
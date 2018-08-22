/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.user.model.acl.SimpleAclRule;

import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ResourceUtils {
    public static final Map LABELS = Collections.singletonMap("foo", "bar");
    public static final String NAMESPACE = "namespace";
    public static final String NAME = "user";
    public static final String CA_NAME = "somename";

    public static KafkaUser createKafkaUser(KafkaUserAuthentication authentication) {
        return new KafkaUserBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withNamespace(NAMESPACE)
                                .withName(NAME)
                                .withLabels(LABELS)
                                .build()
                )
                .withNewSpec()
                .withAuthentication(authentication)
                .withNewKafkaUserAuthorizationSimpleAuthorization()
                    .addNewAcl()
                        .withNewAclRuleTopicResourceResource()
                            .withName("my-topic")
                        .endAclRuleTopicResourceResource()
                        .withOperation(AclOperation.READ)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResourceResource()
                            .withName("my-topic")
                        .endAclRuleTopicResourceResource()
                        .withOperation(AclOperation.DESCRIBE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResourceResource()
                            .withName("my-group")
                        .endAclRuleGroupResourceResource()
                        .withOperation(AclOperation.READ)
                    .endAcl()
                .endKafkaUserAuthorizationSimpleAuthorization()
                .endSpec()
                .build();
    }

    public static KafkaUser createKafkaUserTls() {
        return createKafkaUser(new KafkaUserTlsClientAuthentication());
    }

    public static KafkaUser createKafkaUserScramSha() {
        return createKafkaUser(new KafkaUserScramSha512ClientAuthentication());
    }

    public static Secret createClientsCa()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(CA_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .addToData("clients-ca.key", Base64.getEncoder().encodeToString("clients-ca-key".getBytes()))
                .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .build();
    }

    public static Secret createUserSecretTls()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(Labels.userLabels(LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .addToData("user.key", Base64.getEncoder().encodeToString("expected-key".getBytes()))
                .addToData("user.crt", Base64.getEncoder().encodeToString("expected-crt".getBytes()))
                .build();
    }

    public static Secret createUserSecretScramSha()  {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Labels.userLabels(LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("password", "my-password")
                .build();
    }

    public static Set<SimpleAclRule> createExpectedSimpleAclRules(KafkaUser user) {
        Set<SimpleAclRule> simpleAclRules = new HashSet<SimpleAclRule>();

        if (user.getSpec().getAuthorization() != null && KafkaUserAuthorizationSimple.TYPE_SIMPLE.equals(user.getSpec().getAuthorization().getType())) {
            KafkaUserAuthorizationSimple adapted = (KafkaUserAuthorizationSimple) user.getSpec().getAuthorization();

            if (adapted.getAcls() != null) {
                for (AclRule rule : adapted.getAcls()) {
                    simpleAclRules.add(SimpleAclRule.fromCrd(rule));
                }
            }
        }

        return simpleAclRules;
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.user.UserOperatorConfig.UserOperatorConfigBuilder;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ResourceUtils {
    public static final Map<String, String> LABELS = Collections.singletonMap("foo", "bar");
    public static final String NAMESPACE = "namespace";
    public static final String NAME = "user";
    public static final String CA_CERT_NAME = "ca-cert";
    public static final String CA_KEY_NAME = "ca-key";
    public static final String PASSWORD = "my-password";

    public static UserOperatorConfig createUserOperatorConfig(String namespace, Map<String, String> labels, boolean aclsAdminApiSupported, String scramShaPasswordLength, String secretPrefix) {
        Map<String, String> envVars = new HashMap<>(4);
        envVars.put(UserOperatorConfig.NAMESPACE.key(), namespace);
        envVars.put(UserOperatorConfig.LABELS.key(), labels.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        envVars.put(UserOperatorConfig.CA_CERT_SECRET_NAME.key(), CA_CERT_NAME);
        envVars.put(UserOperatorConfig.CA_KEY_SECRET_NAME.key(), CA_KEY_NAME);
        envVars.put(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED.key(), Boolean.toString(aclsAdminApiSupported));

        if (!scramShaPasswordLength.equals("32")) {
            envVars.put(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key(), scramShaPasswordLength);
        }

        if (secretPrefix != null) {
            envVars.put(UserOperatorConfig.SECRET_PREFIX.key(), secretPrefix);
        }

        return UserOperatorConfig.buildFromMap(envVars);
    }

    public static UserOperatorConfig createUserOperatorConfigForUserControllerTesting(String namespace, Map<String, String> labels, int fullReconciliationInterval, int queueSize, int poolSize, String secretPrefix) {
        return new UserOperatorConfigBuilder(createUserOperatorConfig(namespace, labels, false, "32", secretPrefix))
                      .with(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key(), String.valueOf(fullReconciliationInterval))
                      .with(UserOperatorConfig.WORK_QUEUE_SIZE.key(), String.valueOf(queueSize))
                      .with(UserOperatorConfig.USER_OPERATIONS_THREAD_POOL_SIZE.key(), String.valueOf(poolSize))
                      .build();
    }

    public static UserOperatorConfig createUserOperatorConfig(String namespace) {
        return createUserOperatorConfig(namespace, Map.of(), true, "32", null);
    }

    public static UserOperatorConfig createUserOperatorConfig() {
        return createUserOperatorConfig(NAMESPACE, Map.of(), true, "32", null);
    }

    public static UserOperatorConfig createUserOperatorConfig(String namespace, String scramShaPasswordLength) {
        return new UserOperatorConfigBuilder(createUserOperatorConfig(namespace))
                       .with(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key(), scramShaPasswordLength)
                       .build();
    }

    public static KafkaUser createKafkaUser(String namespace, KafkaUserAuthentication authentication) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(NAME)
                    .withLabels(LABELS)
                .endMetadata()
                .withNewSpec()
                    .withAuthentication(authentication)
                    .withNewKafkaUserAuthorizationSimple()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName("my-topic11").endAclRuleTopicResource()
                            .withOperations(AclOperation.READ, AclOperation.CREATE, AclOperation.WRITE)
                        .endAcl()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName("my-topic")
                            .endAclRuleTopicResource()
                            .withOperations(AclOperation.DESCRIBE, AclOperation.READ)
                        .endAcl()
                    .endKafkaUserAuthorizationSimple()
                    .withNewQuotas()
                        .withConsumerByteRate(1_024 * 1_024)
                        .withProducerByteRate(1_024 * 1_024)
                    .endQuotas()
                .endSpec()
                .build();
    }

    public static KafkaUser createKafkaUser(String namespace, KafkaUserQuotas quotas) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(NAME)
                    .withLabels(LABELS)
                .endMetadata()
                .withNewSpec()
                    .withQuotas(quotas)
                    .withNewKafkaUserAuthorizationSimple()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName("my-topic")
                            .endAclRuleTopicResource()
                        .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
                        .endAcl()
                        .addNewAcl()
                            .withNewAclRuleGroupResource()
                                .withName("my-group")
                            .endAclRuleGroupResource()
                            .withOperations(AclOperation.READ)
                        .endAcl()
                    .endKafkaUserAuthorizationSimple()
                .endSpec()
                .build();
    }

    public static KafkaUser createKafkaUserTls(String namespace) {
        return createKafkaUser(namespace, new KafkaUserTlsClientAuthentication());
    }

    public static KafkaUser createKafkaUserScramSha(String namespace) {
        return createKafkaUser(namespace, new KafkaUserScramSha512ClientAuthentication());
    }

    public static KafkaUser createKafkaUserQuotas(String namespace, Integer consumerByteRate, Integer producerByteRate, Integer requestPercentage, Double controllerMutationRate) {
        KafkaUserQuotas kuq = new KafkaUserQuotasBuilder()
                .withConsumerByteRate(consumerByteRate)
                .withProducerByteRate(producerByteRate)
                .withRequestPercentage(requestPercentage)
                .withControllerMutationRate(controllerMutationRate)
                .build();

        return createKafkaUser(namespace, kuq);
    }

    public static Secret createClientsCaCertSecret(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.CA_CERT_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .build();
    }

    public static Secret createClientsCaKeySecret(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.CA_KEY_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("ca.key", Base64.getEncoder().encodeToString("clients-ca-key".getBytes()))
                .build();
    }

    public static Secret createUserSecretTls(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(namespace)
                    .withLabels(Labels.fromMap(LABELS)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(NAME)
                        .withKubernetesPartOf(NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .toMap())
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .addToData("user.key", Base64.getEncoder().encodeToString("expected-key".getBytes()))
                .addToData("user.crt", Base64.getEncoder().encodeToString("expected-crt".getBytes()))
                .addToData("user.p12", Base64.getEncoder().encodeToString("expected-p12".getBytes()))
                .addToData("user.password", Base64.getEncoder().encodeToString("expected-password".getBytes()))
                .build();
    }

    public static Secret createUserSecretScramSha(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(namespace)
                    .withLabels(Labels.fromMap(LABELS).withStrimziKind(KafkaUser.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData(KafkaUserModel.KEY_PASSWORD, Base64.getEncoder().encodeToString(PASSWORD.getBytes()))
                .addToData(KafkaUserModel.KEY_SASL_JAAS_CONFIG, Base64.getEncoder().encodeToString(KafkaUserModel.getSaslJsonConfig(NAME, PASSWORD).getBytes()))
                .build();
    }

    public static Set<SimpleAclRule> createExpectedSimpleAclRules(KafkaUser user) {
        Set<SimpleAclRule> simpleAclRules = new HashSet<>();

        if (user.getSpec().getAuthorization() != null && KafkaUserAuthorizationSimple.TYPE_SIMPLE.equals(user.getSpec().getAuthorization().getType())) {
            KafkaUserAuthorizationSimple adapted = (KafkaUserAuthorizationSimple) user.getSpec().getAuthorization();

            if (adapted.getAcls() != null) {
                for (AclRule rule : adapted.getAcls()) {
                    simpleAclRules.addAll(SimpleAclRule.fromCrd(rule));
                }
            }
        }

        return simpleAclRules;
    }
}

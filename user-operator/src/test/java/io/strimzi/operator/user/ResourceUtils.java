/*
 * Copyright Strimzi authors.
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
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.operator.common.model.Labels;
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

    public static UserOperatorConfig createUserOperatorConfig(Map<String, String> labels, boolean aclsAdminApiSupported, boolean useKRaft, String scramShaPasswordLength, String secretPrefix) {
        Map<String, String> envVars = new HashMap<>(4);
        envVars.put(UserOperatorConfig.STRIMZI_NAMESPACE, NAMESPACE);
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, labels.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        envVars.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, CA_CERT_NAME);
        envVars.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, CA_KEY_NAME);
        envVars.put(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED, Boolean.toString(aclsAdminApiSupported));
        envVars.put(UserOperatorConfig.STRIMZI_KRAFT_ENABLED, Boolean.toString(useKRaft));

        if (!scramShaPasswordLength.equals("32")) {
            envVars.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, scramShaPasswordLength);
        }

        if (secretPrefix != null) {
            envVars.put(UserOperatorConfig.STRIMZI_SECRET_PREFIX, secretPrefix);
        }

        return UserOperatorConfig.fromMap(envVars);
    }

    public static UserOperatorConfig createUserOperatorConfigForUserControllerTesting(Map<String, String> labels, int fullReconciliationInterval, int queueSize, int poolSize, String secretPrefix) {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(UserOperatorConfig.STRIMZI_NAMESPACE, NAMESPACE);
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, labels.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        envVars.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, CA_CERT_NAME);
        envVars.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, CA_KEY_NAME);
        envVars.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, CA_KEY_NAME);
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(fullReconciliationInterval));
        envVars.put(UserOperatorConfig.STRIMZI_WORK_QUEUE_SIZE, Integer.toString(queueSize));
        envVars.put(UserOperatorConfig.STRIMZI_CONTROLLER_THREAD_POOL_SIZE, Integer.toString(poolSize));
        envVars.put(UserOperatorConfig.STRIMZI_SECRET_PREFIX, secretPrefix);

        return UserOperatorConfig.fromMap(envVars);
    }

    public static UserOperatorConfig createUserOperatorConfig() {
        return createUserOperatorConfig(Map.of(), true, false, "32", null);
    }

    public static UserOperatorConfig createUserOperatorConfig(String scramShaPasswordLength) {
        return createUserOperatorConfig(Map.of(), true, false, scramShaPasswordLength, null);
    }

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

    public static KafkaUser createKafkaUser(KafkaUserQuotas quotas) {
        return new KafkaUserBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withNamespace(NAMESPACE)
                                .withName(NAME)
                                .withLabels(LABELS)
                                .build()
                )
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

    public static KafkaUser createKafkaUserTls() {
        return createKafkaUser(new KafkaUserTlsClientAuthentication());
    }

    public static KafkaUser createKafkaUserScramSha() {
        return createKafkaUser(new KafkaUserScramSha512ClientAuthentication());
    }

    public static KafkaUser createKafkaUserQuotas(Integer consumerByteRate, Integer producerByteRate, Integer requestPercentage, Double controllerMutationRate) {
        KafkaUserQuotas kuq = new KafkaUserQuotasBuilder()
                .withConsumerByteRate(consumerByteRate)
                .withProducerByteRate(producerByteRate)
                .withRequestPercentage(requestPercentage)
                .withControllerMutationRate(controllerMutationRate)
                .build();

        return createKafkaUser(kuq);
    }

    public static Secret createClientsCaCertSecret()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.CA_CERT_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .build();
    }

    public static Secret createClientsCaKeySecret()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.CA_KEY_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .addToData("ca.key", Base64.getEncoder().encodeToString("clients-ca-key".getBytes()))
                .build();
    }

    public static Secret createUserSecretTls()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
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

    public static Secret createUserSecretScramSha()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
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

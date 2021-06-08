/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.KafkaUserSpec;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.UserOperatorConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;

import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaUserModelTest {
    private final KafkaUser tlsUser = ResourceUtils.createKafkaUserTls();
    private final KafkaUser scramShaUser = ResourceUtils.createKafkaUserScramSha();
    private final KafkaUser quotasUser = ResourceUtils.createKafkaUserQuotas(1000, 2000, 42, 10);
    private final Secret clientsCaCert = ResourceUtils.createClientsCaCertSecret();
    private final Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
    private final CertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences(), hasSize(1));
        assertThat(resource.getMetadata().getOwnerReferences(), hasItem(ownerRef));
    }

    @Test
    public void testFromCrdTlsUser()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);

        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        assertThat(model.authentication.getType(), is(KafkaUserTlsClientAuthentication.TYPE_TLS));

        assertThat(model.getSimpleAclRules(), hasSize(ResourceUtils.createExpectedSimpleAclRules(tlsUser).size()));
        assertThat(model.getSimpleAclRules(), is(ResourceUtils.createExpectedSimpleAclRules(tlsUser)));
    }

    @Test
    public void testFromCrdQuotaUser()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, quotasUser, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);

        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        KafkaUserQuotas quotas = quotasUser.getSpec().getQuotas();
        assertThat(model.getQuotas().getConsumerByteRate(), is(quotas.getConsumerByteRate()));
        assertThat(model.getQuotas().getProducerByteRate(), is(quotas.getProducerByteRate()));
        assertThat(model.getQuotas().getRequestPercentage(), is(quotas.getRequestPercentage()));
        assertThat(model.getQuotas().getControllerMutationRate(), is(quotas.getControllerMutationRate()));
    }

    @Test
    public void testFromCrdQuotaUserWithNullValues()   {
        KafkaUser quotasUserWithNulls = ResourceUtils.createKafkaUserQuotas(null, 2000, null, 10);
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, quotasUserWithNulls, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.fromMap(ResourceUtils.LABELS)
                .withStrimziKind(KafkaUser.RESOURCE_KIND)
                .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                .withKubernetesInstance(ResourceUtils.NAME)
                .withKubernetesPartOf(ResourceUtils.NAME)
                .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        assertThat(model.getQuotas().getConsumerByteRate(), is(nullValue()));
        assertThat(model.getQuotas().getProducerByteRate(), is(2000));
        assertThat(model.getQuotas().getRequestPercentage(), is(nullValue()));
        assertThat(model.getQuotas().getControllerMutationRate(), is(10));

    }

    @Test
    public void testGenerateSecret()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generatedSecret = model.generateSecret();

        assertThat(generatedSecret.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(generatedSecret.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generatedSecret.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generatedSecret.getMetadata().getAnnotations(), is(emptyMap()));
        assertThat(generatedSecret.getMetadata().getLabels(),
                is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretWithPrefix()    {
        String secretPrefix = "strimzi-";
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null, secretPrefix);
        Secret generatedSecret = model.generateSecret();

        assertThat(generatedSecret.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(generatedSecret.getMetadata().getName(), is(secretPrefix + ResourceUtils.NAME));
        assertThat(generatedSecret.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generatedSecret.getMetadata().getAnnotations(), is(emptyMap()));
        assertThat(generatedSecret.getMetadata().getLabels(),
                is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }


    @Test
    public void testGenerateSecretWithMetadataOverrides()    {
        KafkaUser userWithTemplate = new KafkaUserBuilder(tlsUser)
                .editSpec()
                    .withNewTemplate()
                        .withNewSecret()
                            .withNewMetadata()
                                .withLabels(singletonMap("label1", "value1"))
                                .withAnnotations(singletonMap("anno1", "value1"))
                            .endMetadata()
                        .endSecret()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, userWithTemplate, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generatedSecret = model.generateSecret();

        assertThat(generatedSecret.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(generatedSecret.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generatedSecret.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generatedSecret.getMetadata().getLabels(),
                is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withAdditionalLabels(singletonMap("label1", "value1"))
                        .toMap()));
        assertThat(generatedSecret.getMetadata().getAnnotations(), is(singletonMap("anno1", "value1")));
        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratesCertificateWhenNoSecretExistsProvidedByUser()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generated = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")), is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is("crt file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is("key file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is("key store"));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateSecretGeneratesCertificateAtCaChange() {
        Secret userCert = ResourceUtils.createUserSecretTls();
        Secret clientsCaCertSecret = ResourceUtils.createClientsCaCertSecret();
        clientsCaCertSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));

        Secret clientsCaKeySecret = ResourceUtils.createClientsCaKeySecret();
        clientsCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));

        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCertSecret, clientsCaKeySecret, userCert, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generatedSecret = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generatedSecret, "ca.crt")),  is("different-clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.crt")), is("crt file"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.key")), is("key file"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.p12")), is("key store"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratedCertificateDoesNotChangeFromUserProvided()    {
        Secret userCert = ResourceUtils.createUserSecretTls();

        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, userCert, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generatedSecret = model.generateSecret();

        // These values match those in ResourceUtils.createUserSecretTls()
        assertThat(new String(model.decodeFromSecret(generatedSecret, "ca.crt")),  is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.crt")), is("expected-crt"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.key")), is("expected-key"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.p12")), is("expected-p12"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.password")), is("expected-password"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratesCertificateWithExistingScramSha()    {
        Secret userCert = ResourceUtils.createUserSecretScramSha();
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, userCert, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generated = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")),  is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is("crt file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is("key file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is("key store"));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateSecretGeneratesKeyStoreWhenOldVersionSecretExists() {
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret oldSecret = model.generateSecret();

        // remove keystore and password to simulate a Secret from a previous version
        oldSecret.getData().remove("user.p12");
        oldSecret.getData().remove("user.password");

        model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, oldSecret, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generatedSecret = model.generateSecret();

        assertThat(generatedSecret.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(new String(model.decodeFromSecret(generatedSecret, "ca.crt")), is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.crt")), is("crt file"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.key")), is("key file"));
        // assert that keystore and password have been re-added
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.p12")), is("key store"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratesPasswordWhenNoUserSecretExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generatedSecret = model.generateSecret();

        assertThat(generatedSecret.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generatedSecret.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generatedSecret.getMetadata().getLabels(),
                is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));

        assertThat(generatedSecret.getData().keySet(), is(new HashSet<>(Arrays.asList(KafkaUserModel.KEY_PASSWORD, KafkaUserModel.KEY_SASL_JAAS_CONFIG))));

        String password = "aaaaaaaaaa";
        assertThat(new String(Base64.getDecoder().decode(generatedSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(password));
        assertThat(new String(Base64.getDecoder().decode(generatedSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))),
                is("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + ResourceUtils.NAME + "\" password=\"" + password + "\";"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratesPasswordKeepingExistingScramShaPassword()    {
        Secret scramShaSecret = ResourceUtils.createUserSecretScramSha();
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, scramShaSecret, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generated = model.generateSecret();

        assertThat(generated.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generated.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generated.getMetadata().getLabels(),
                is(Labels.fromMap(ResourceUtils.LABELS)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .toMap()));
        assertThat(generated.getData().keySet(), is(new HashSet<>(Arrays.asList(KafkaUserModel.KEY_PASSWORD, KafkaUserModel.KEY_SASL_JAAS_CONFIG))));
        assertThat(generated.getData(), hasEntry(KafkaUserModel.KEY_PASSWORD, scramShaSecret.getData().get(KafkaUserModel.KEY_PASSWORD)));
        assertThat(generated.getData(), hasEntry(KafkaUserModel.KEY_SASL_JAAS_CONFIG, scramShaSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG)));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateSecretGeneratesPasswordFromExistingTlsSecret()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, userCert, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        Secret generated = model.generateSecret();

        assertThat(generated.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generated.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generated.getMetadata().getLabels(),
                is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));

        assertThat(generated.getData().keySet(), is(new HashSet<>(Arrays.asList(KafkaUserModel.KEY_PASSWORD, KafkaUserModel.KEY_SASL_JAAS_CONFIG))));

        String password = "aaaaaaaaaa";
        assertThat(new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_PASSWORD))), is(password));
        assertThat(new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))),
                is("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + ResourceUtils.NAME + "\" password=\"" + password + "\";"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateSecretWithNoTlsAuthenticationKafkaUserReturnsNull()    {
        Secret userCert = ResourceUtils.createUserSecretTls();

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());

        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, user, clientsCaCert, clientsCaKey, userCert, UserOperatorConfig.DEFAULT_SECRET_PREFIX);

        assertThat(model.generateSecret(), is(nullValue()));
    }

    @Test
    public void testGetSimpleAclRulesWithNoSimpleAuthorizationReturnsNull()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());

        KafkaUserModel model = KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, user, clientsCaCert, clientsCaKey, userCert, UserOperatorConfig.DEFAULT_SECRET_PREFIX);

        assertThat(model.getSimpleAclRules(), is(nullValue()));
    }

    @Test
    public void testDecodeUsername()    {
        assertThat(KafkaUserModel.decodeUsername("CN=my-user"), is("my-user"));
        assertThat(KafkaUserModel.decodeUsername("CN=my-user,OU=my-org"), is("my-user"));
        assertThat(KafkaUserModel.decodeUsername("OU=my-org,CN=my-user"), is("my-user"));
    }

    @Test
    public void testGetUsername()    {
        assertThat(KafkaUserModel.getTlsUserName("my-user"), is("CN=my-user"));
        assertThat(KafkaUserModel.getScramUserName("my-user"), is("my-user"));
    }

    @Test
    public void testFromCrdTlsUserWith65CharTlsUsernameThrows()    {
        KafkaUser tooLong = new KafkaUserBuilder(tlsUser)
                .editMetadata()
                    .withName("User-123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            // 65 characters => Should throw exception with TLS
            KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tooLong, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
        });
    }

    @Test
    public void testFromCrdTlsUserWith64CharTlsUsernameValid()    {
        // 64 characters => Should be still OK
        KafkaUser notTooLong = new KafkaUserBuilder(tlsUser)
                .editMetadata()
                    .withName("User123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, notTooLong, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
    }

    @Test
    public void testFromCrdScramShaUserWith65CharSaslUsernameValid()    {
        // 65 characters => should work with SCRAM-SHA-512
        KafkaUser tooLong = new KafkaUserBuilder(scramShaUser)
                .editMetadata()
                    .withName("User-123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, tooLong, clientsCaCert, clientsCaKey, null, UserOperatorConfig.DEFAULT_SECRET_PREFIX);
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.KafkaUserSpec;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsExternalClientAuthentication;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.UserOperatorConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;

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
    private final static String DESIRED_PASSWORD = "123456";
    private final static String DESIRED_BASE64_PASSWORD = Base64.getEncoder().encodeToString(DESIRED_PASSWORD.getBytes(StandardCharsets.UTF_8));

    private final KafkaUser tlsUser = ResourceUtils.createKafkaUserTls();
    private final KafkaUser scramShaUser = ResourceUtils.createKafkaUserScramSha();
    private final KafkaUser quotasUser = ResourceUtils.createKafkaUserQuotas(1000, 2000, 42, 10d);
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
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

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
    public void testFromCrdTlsExternalUser()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(ResourceUtils.createKafkaUser(new KafkaUserTlsExternalClientAuthentication()), UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.fromMap(ResourceUtils.LABELS)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        assertThat(model.authentication.getType(), is(KafkaUserTlsExternalClientAuthentication.TYPE_TLS_EXTERNAL));
        assertThat(model.isTlsExternalUser(), is(true));
        assertThat(model.getUserName(), is("CN=" + ResourceUtils.NAME));
        assertThat(model.generateSecret(), is(nullValue()));
    }

    @Test
    public void testFromCrdQuotaUser()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(quotasUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

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
        KafkaUser quotasUserWithNulls = ResourceUtils.createKafkaUserQuotas(null, 2000, null, 10d);
        KafkaUserModel model = KafkaUserModel.fromCrd(quotasUserWithNulls, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

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
        assertThat(model.getQuotas().getControllerMutationRate(), is(10d));

    }

    @Test
    public void testGenerateSecret()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
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
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, secretPrefix, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
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

        KafkaUserModel model = KafkaUserModel.fromCrd(userWithTemplate, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
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
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
        Secret generated = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")), is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is(MockCertManager.userCert()));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is(MockCertManager.userKey()));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is(MockCertManager.userKeyStore()));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testMissingOrWrongCaSecrets()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        Secret emptySecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("dummy-ca-secret")
                .endMetadata()
                .withData(Map.of())
                .build();

        InvalidCertificateException e = assertThrows(InvalidCertificateException.class, () -> {
            model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, null, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
        });
        assertThat(e.getMessage(), is("The Clients CA Cert Secret is missing"));

        e = assertThrows(InvalidCertificateException.class, () -> {
            model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, null, null, 365, 30, null, Clock.systemUTC());
        });
        assertThat(e.getMessage(), is("The Clients CA Key Secret is missing"));

        e = assertThrows(InvalidCertificateException.class, () -> {
            model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, emptySecret, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
        });
        assertThat(e.getMessage(), is("The Clients CA Cert Secret is missing the ca.crt file"));

        e = assertThrows(InvalidCertificateException.class, () -> {
            model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, emptySecret, null, 365, 30, null, Clock.systemUTC());
        });
        assertThat(e.getMessage(), is("The Clients CA Key Secret is missing the ca.key file"));
    }

    @Test
    public void testGenerateSecretGeneratesCertificateAtCaChange() {
        Secret userCert = ResourceUtils.createUserSecretTls();
        Secret clientsCaCertSecret = ResourceUtils.createClientsCaCertSecret();
        clientsCaCertSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));

        Secret clientsCaKeySecret = ResourceUtils.createClientsCaKeySecret();
        clientsCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));

        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCertSecret, clientsCaKeySecret, userCert, 365, 30, null, Clock.systemUTC());
        Secret generatedSecret = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generatedSecret, "ca.crt")),  is("different-clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.crt")), is(MockCertManager.userCert()));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.key")), is(MockCertManager.userKey()));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.p12")), is(MockCertManager.userKeyStore()));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratedCertificateDoesNotChangeFromUserProvided()    {
        Secret userCert = ResourceUtils.createUserSecretTls();

        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userCert, 365, 30, null, Clock.systemUTC());
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
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userCert, 365, 30, null, Clock.systemUTC());
        Secret generated = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")),  is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is(MockCertManager.userCert()));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is(MockCertManager.userKey()));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is(MockCertManager.userKeyStore()));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateSecretGeneratesKeyStoreWhenOldVersionSecretExists() {
        KafkaUserModel model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, null, 365, 30, null, Clock.systemUTC());
        Secret oldSecret = model.generateSecret();

        // remove keystore and password to simulate a Secret from a previous version
        oldSecret.getData().remove("user.p12");
        oldSecret.getData().remove("user.password");

        model = KafkaUserModel.fromCrd(tlsUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, oldSecret, 365, 30, null, Clock.systemUTC());
        Secret generatedSecret = model.generateSecret();

        assertThat(generatedSecret.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(new String(model.decodeFromSecret(generatedSecret, "ca.crt")), is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.crt")), is(MockCertManager.userCert()));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.key")), is(MockCertManager.userKey()));
        // assert that keystore and password have been re-added
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.p12")), is(MockCertManager.userKeyStore()));
        assertThat(new String(model.decodeFromSecret(generatedSecret, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretGeneratesPasswordWhenNoUserSecretExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(scramShaUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, null, null);
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
        KafkaUserModel model = KafkaUserModel.fromCrd(scramShaUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, scramShaSecret, null);
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
    public void testGenerateSecretUseDesiredPasswordWhenSpecified()    {
        KafkaUser user = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("my-password", "my-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        Secret desiredPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-password", DESIRED_BASE64_PASSWORD)
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, null, desiredPasswordSecret);
        Secret generatedSecret = model.generateSecret();

        assertThat(model.getScramSha512Password(), is(DESIRED_PASSWORD));
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

        assertThat(new String(Base64.getDecoder().decode(generatedSecret.getData().get(KafkaUserModel.KEY_PASSWORD))), is(DESIRED_PASSWORD));
        assertThat(new String(Base64.getDecoder().decode(generatedSecret.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))),
                is("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + ResourceUtils.NAME + "\" password=\"" + DESIRED_PASSWORD + "\";"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generatedSecret);
    }

    @Test
    public void testGenerateSecretUpdatesPasswordWhenRequestedByTheUser()    {
        Secret scramShaSecret = ResourceUtils.createUserSecretScramSha();

        KafkaUser user = new KafkaUserBuilder(scramShaUser)
            .editSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                    .withNewPassword()
                        .withNewValueFrom()
                            .withNewSecretKeyRef("my-password", "my-secret", false)
                        .endValueFrom()
                    .endPassword()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec()
            .build();

        Secret desiredPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-password", DESIRED_BASE64_PASSWORD)
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, scramShaSecret, desiredPasswordSecret);
        Secret generated = model.generateSecret();

        assertThat(model.getScramSha512Password(), is(DESIRED_PASSWORD));
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
        assertThat(new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_PASSWORD))), is(DESIRED_PASSWORD));
        assertThat(new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_SASL_JAAS_CONFIG))),
                is("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + ResourceUtils.NAME + "\" password=\"" + DESIRED_PASSWORD + "\";"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateSecretUseDesiredPasswordMissingKey()    {
        KafkaUser user = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("my-password", "my-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        Secret desiredPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-other-password", DESIRED_BASE64_PASSWORD)
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> {
            model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, null, desiredPasswordSecret);
        });

        assertThat(e.getMessage(), is("Secret my-secret does not contain the key my-password with requested user password."));
    }

    @Test
    public void testGenerateSecretUseDesiredPasswordIsEmpty()    {
        KafkaUser user = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("my-password", "my-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        Secret desiredPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-password", Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)))
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> {
            model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, null, desiredPasswordSecret);
        });

        assertThat(e.getMessage(), is("The requested user password is empty."));
    }

    @Test
    public void testGenerateSecretUseDesiredPasswordSecretDoesNotExist()    {
        KafkaUser user = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("my-password", "my-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> {
            model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, null, null);
        });

        assertThat(e.getMessage(), is("Secret my-secret with requested user password does not exist."));
    }

    @Test
    public void testGenerateSecretGeneratesPasswordFromExistingTlsSecret()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(scramShaUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        model.maybeGeneratePassword(Reconciliation.DUMMY_RECONCILIATION, passwordGenerator, userCert, null);
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
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        assertThat(model.generateSecret(), is(nullValue()));
    }

    @Test
    public void testGetSimpleAclRulesWithNoSimpleAuthorizationReturnsNull()    {
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

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
            KafkaUserModel.fromCrd(tooLong, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
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

        KafkaUserModel.fromCrd(notTooLong, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
    }

    @Test
    public void testFromCrdScramShaUserWith65CharSaslUsernameValid()    {
        // 65 characters => should work with SCRAM-SHA-512
        KafkaUser tooLong = new KafkaUserBuilder(scramShaUser)
                .editMetadata()
                    .withName("User-123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(tooLong, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
    }

    @Test
    public void testFromCrdScramShaUserWithEmptyPasswordThrows()    {
        KafkaUser emptyPassword = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> {
            KafkaUserModel.fromCrd(emptyPassword, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        });

        assertThat(e.getMessage(), is("Resource requests custom SCRAM-SHA-512 password but doesn't specify the secret name and/or key"));
    }

    @Test
    public void testFromCrdScramShaUserWithMissingPasswordKeyThrows()    {
        KafkaUser missingKey = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef(null, "my-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> {
            KafkaUserModel.fromCrd(missingKey, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);
        });

        assertThat(e.getMessage(), is("Resource requests custom SCRAM-SHA-512 password but doesn't specify the secret name and/or key"));
    }

    @Test
    public void testFromCrdScramShaUserWithPasswordParsing()    {
        KafkaUser user = new KafkaUserBuilder(scramShaUser)
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("my-password", "my-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(user, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        assertThat(model.isUserWithDesiredPassword(), is(true));
        assertThat(model.desiredPasswordSecretKey(), is("my-password"));
        assertThat(model.desiredPasswordSecretName(), is("my-secret"));
    }

    @Test
    public void testFromCrdScramShaUserWithoutPasswordParsing()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(scramShaUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED, false);

        assertThat(model.isUserWithDesiredPassword(), is(false));
        assertThat(model.desiredPasswordSecretKey(), is(nullValue()));
        assertThat(model.desiredPasswordSecretName(), is(nullValue()));
    }

    @Test
    public void testFromCrdAclsWithAclsAdminApiSupportMissing()    {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaUserModel.fromCrd(scramShaUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, false, false));
        assertThat(e.getMessage(), is("Simple authorization ACL rules are configured but not supported in the Kafka cluster configuration."));
    }

    @Test
    public void testFromCrdScramSha512NotSupportedInKraft()    {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaUserModel.fromCrd(scramShaUser, UserOperatorConfig.DEFAULT_SECRET_PREFIX, true, true));
        assertThat(e.getMessage(), is("SCRAM-SHA-512 authentication is currently not supported in KRaft based Kafka clusters."));
    }
}

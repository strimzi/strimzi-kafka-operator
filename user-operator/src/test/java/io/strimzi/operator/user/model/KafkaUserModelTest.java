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
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.common.PasswordGenerator;
import static org.hamcrest.CoreMatchers.is;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaUserModelTest {
    private final KafkaUser tlsUser = ResourceUtils.createKafkaUserTls();
    private final KafkaUser scramShaUser = ResourceUtils.createKafkaUserScramSha();
    private final KafkaUser quotasUser = ResourceUtils.createKafkaUserQuotas(1000, 2000, 42);
    private final Secret clientsCaCert = ResourceUtils.createClientsCaCertSecret();
    private final Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
    private final CertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    @Test
    public void testFromCrd()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);

        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.userLabels(ResourceUtils.LABELS)
                        .withKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName()
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        assertThat(model.authentication.getType(), is(KafkaUserTlsClientAuthentication.TYPE_TLS));

        assertThat(model.getSimpleAclRules().size(), is(ResourceUtils.createExpectedSimpleAclRules(tlsUser).size()));
        assertThat(model.getSimpleAclRules(), is(ResourceUtils.createExpectedSimpleAclRules(tlsUser)));
    }

    @Test
    public void testQuotasFromCrd()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, quotasUser, clientsCaCert, clientsCaKey, null);

        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.userLabels(ResourceUtils.LABELS)
                        .withKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName()
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        KafkaUserQuotas quotas = quotasUser.getSpec().getQuotas();
        assertThat(model.getQuotas().getConsumerByteRate(), is(quotas.getConsumerByteRate()));
        assertThat(model.getQuotas().getProducerByteRate(), is(quotas.getProducerByteRate()));
        assertThat(model.getQuotas().getRequestPercentage(), is(quotas.getRequestPercentage()));
    }

    @Test
    public void testQuotasFromCrdNullValues()   {
        KafkaUser quotasUserWithNulls = ResourceUtils.createKafkaUserQuotas(null, 2000, null);
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, quotasUserWithNulls, clientsCaCert, clientsCaKey, null);

        assertThat(model.namespace, is(ResourceUtils.NAMESPACE));
        assertThat(model.name, is(ResourceUtils.NAME));
        assertThat(model.labels, is(Labels.userLabels(ResourceUtils.LABELS)
                .withKind(KafkaUser.RESOURCE_KIND)
                .withKubernetesName()
                .withKubernetesInstance(ResourceUtils.NAME)
                .withKubernetesPartOf(ResourceUtils.NAME)
                .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)));
        KafkaUserQuotas quotas = quotasUserWithNulls.getSpec().getQuotas();
        assertThat(model.getQuotas().getConsumerByteRate(), is(quotas.getConsumerByteRate()));
        assertThat(model.getQuotas().getProducerByteRate(), is(quotas.getProducerByteRate()));
        assertThat(model.getQuotas().getRequestPercentage(), is(quotas.getRequestPercentage()));
    }

    @Test
    public void testGenerateSecret()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);
        Secret generated = model.generateSecret();

        assertThat(generated.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(generated.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generated.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generated.getMetadata().getLabels(),
                is(Labels.userLabels(ResourceUtils.LABELS)
                        .withKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName()
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));
        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateWhenNoExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);
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
    public void testGenerateCertificateAtCaChange()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        Secret clientsCaCertSecret = ResourceUtils.createClientsCaCertSecret();
        clientsCaCertSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));
        Secret clientsCaKeSecret = ResourceUtils.createClientsCaKeySecret();
        clientsCaKeSecret.getData().put("ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));

        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCertSecret, clientsCaKeSecret, userCert);
        Secret generated = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")),  is("different-clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is("crt file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is("key file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is("key store"));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateKeepExisting()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, userCert);
        Secret generated = model.generateSecret();

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")),  is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is("expected-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is("expected-key"));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is("expected-p12"));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("expected-password"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateExistingScramSha()    {
        Secret userCert = ResourceUtils.createUserSecretScramSha();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, userCert);
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
    public void testGenerateKeyStoreWhenOldVersionSecretExists() {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);
        Secret oldSecret = model.generateSecret();
        // removing keystore and password to simulate a Secret from a previous version
        oldSecret.getData().remove("user.p12");
        oldSecret.getData().remove("user.password");

        model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, oldSecret);
        Secret generated = model.generateSecret();

        assertThat(generated.getData().keySet(), is(set("ca.crt", "user.crt", "user.key", "user.p12", "user.password")));

        assertThat(new String(model.decodeFromSecret(generated, "ca.crt")), is("clients-ca-crt"));
        assertThat(new String(model.decodeFromSecret(generated, "user.crt")), is("crt file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.key")), is("key file"));
        assertThat(new String(model.decodeFromSecret(generated, "user.p12")), is("key store"));
        assertThat(new String(model.decodeFromSecret(generated, "user.password")), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGeneratePasswordWhenNoSecretExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, null);
        Secret generated = model.generateSecret();

        assertThat(generated.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generated.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generated.getMetadata().getLabels(),
                is(Labels.userLabels(ResourceUtils.LABELS)
                        .withKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName()
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));

        assertThat(generated.getData().keySet(), is(singleton(KafkaUserModel.KEY_PASSWORD)));
        assertThat(new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_PASSWORD))), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGeneratePasswordKeepExistingScramSha()    {
        Secret userPassword = ResourceUtils.createUserSecretScramSha();
        String existing = userPassword.getData().get(KafkaUserModel.KEY_PASSWORD);
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, userPassword);
        Secret generated = model.generateSecret();

        assertThat(generated.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generated.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generated.getMetadata().getLabels(),
                is(Labels.userLabels(ResourceUtils.LABELS)
                        .withKubernetesName()
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKind(KafkaUser.RESOURCE_KIND)
                        .toMap()));
        assertThat(generated.getData().keySet(), is(singleton(KafkaUserModel.KEY_PASSWORD)));
        assertThat(generated.getData().get(KafkaUserModel.KEY_PASSWORD), is(existing));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGeneratePasswordExistingTlsSecret()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, userCert);
        Secret generated = model.generateSecret();

        assertThat(generated.getMetadata().getName(), is(ResourceUtils.NAME));
        assertThat(generated.getMetadata().getNamespace(), is(ResourceUtils.NAMESPACE));
        assertThat(generated.getMetadata().getLabels(),
                is(Labels.userLabels(ResourceUtils.LABELS)
                        .withKind(KafkaUser.RESOURCE_KIND)
                        .withKubernetesName()
                        .withKubernetesInstance(ResourceUtils.NAME)
                        .withKubernetesPartOf(ResourceUtils.NAME)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .toMap()));

        assertThat(generated.getData().keySet(), is(singleton("password")));
        assertThat(new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_PASSWORD))), is("aaaaaaaaaa"));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testNoTlsAuthn()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, user, clientsCaCert, clientsCaKey, userCert);

        assertThat(model.generateSecret(), is(nullValue()));
    }

    @Test
    public void testNoSimpleAuthz()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, user, clientsCaCert, clientsCaKey, userCert);

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
    public void test65CharTlsUsername()    {
        assertThrows(InvalidResourceException.class, () -> {
            // 65 characters => Should throw exception with TLS
            KafkaUser tooLong = new KafkaUserBuilder(tlsUser)
                    .editMetadata()
                        .withName("User-123456789012345678901234567890123456789012345678901234567890")
                    .endMetadata()
                    .build();

            KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tooLong, clientsCaCert, clientsCaKey, null);
        });
    }

    @Test
    public void test64CharTlsUsername()    {
        // 64 characters => Should be still OK
        KafkaUser notTooLong = new KafkaUserBuilder(tlsUser)
                .editMetadata()
                    .withName("User123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, notTooLong, clientsCaCert, clientsCaKey, null);
    }

    @Test
    public void test65CharSaslUsername()    {
        // 65 characters => should work with SCRAM-SHA-512
        KafkaUser tooLong = new KafkaUserBuilder(scramShaUser)
                .editMetadata()
                    .withName("User-123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tooLong, clientsCaCert, clientsCaKey, null);
    }
}

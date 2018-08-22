/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserSpec;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.operator.PasswordGenerator;
import org.junit.Test;

import java.util.Base64;

import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaUserModelTest {
    private final KafkaUser tlsUser = ResourceUtils.createKafkaUserTls();
    private final KafkaUser scramShaUser = ResourceUtils.createKafkaUserScramSha();
    private final Secret clientsCa = ResourceUtils.createClientsCa();
    private final CertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a");

    @Test
    public void testFromCrd()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCa, null);

        assertEquals(ResourceUtils.NAMESPACE, model.namespace);
        assertEquals(ResourceUtils.NAME, model.name);
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND), model.labels);
        assertEquals(KafkaUserTlsClientAuthentication.TYPE_TLS, model.authentication.getType());

        KafkaUserAuthorizationSimple simple = (KafkaUserAuthorizationSimple) tlsUser.getSpec().getAuthorization();
        assertEquals(ResourceUtils.createExpectedSimpleAclRules(tlsUser).size(), model.getSimpleAclRules().size());
        assertEquals(ResourceUtils.createExpectedSimpleAclRules(tlsUser), model.getSimpleAclRules());
    }

    @Test
    public void testGenerateSecret()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCa, null);
        Secret generated = model.generateSecret();

        assertEquals(set("ca.crt", "user.crt", "user.key"), generated.getData().keySet());

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());
    }

    @Test
    public void testGenerateCertificateWhenNoExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCa, null);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testGenerateCertificateAtCaChange()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        Secret clientsCa = ResourceUtils.createClientsCa();
        clientsCa.getData().put("clients-ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));
        clientsCa.getData().put("clients-ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));

        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals("different-clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testGenerateCertificateKeepExisting()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("expected-crt", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("expected-key", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testGenerateCertificateExistingScramSha()    {
        Secret userCert = ResourceUtils.createUserSecretScramSha();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testGeneratePasswordWhenNoSecretExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCa, null);
        Secret generated = model.generateSecret();

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        assertEquals(singleton(KafkaUserModel.KEY_PASSWORD), generated.getData().keySet());
        assertEquals("aaaaaaaaaa", generated.getData().get(KafkaUserModel.KEY_PASSWORD));
    }

    @Test
    public void testGeneratePasswordKeepExistingScramSha()    {
        Secret userCert = ResourceUtils.createUserSecretScramSha();
        String existing = userCert.getData().get(KafkaUserModel.KEY_PASSWORD);
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        assertEquals(singleton(KafkaUserModel.KEY_PASSWORD), generated.getData().keySet());
        assertEquals(existing, generated.getData().get(KafkaUserModel.KEY_PASSWORD));
    }

    @Test
    public void testGeneratePasswordExistingTlsSecret()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        assertEquals(singleton("password"), generated.getData().keySet());
        assertEquals("aaaaaaaaaa", generated.getData().get(KafkaUserModel.KEY_PASSWORD));
    }

    @Test
    public void testNoTlsAuthn()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, user, clientsCa, userCert);

        assertNull(model.generateSecret());
    }

    @Test
    public void testNoSimpleAuthz()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, user, clientsCa, userCert);

        assertNull(model.getSimpleAclRules());
    }

    @Test
    public void testDecodeUsername()    {
        assertEquals("my-user", KafkaUserModel.decodeUsername("CN=my-user"));
        assertEquals("my-user", KafkaUserModel.decodeUsername("CN=my-user,OU=my-org"));
        assertEquals("my-user", KafkaUserModel.decodeUsername("OU=my-org,CN=my-user"));
    }

    @Test
    public void testGetUsername()    {
        assertEquals("CN=my-user", KafkaUserModel.getUserName("my-user"));
    }
}

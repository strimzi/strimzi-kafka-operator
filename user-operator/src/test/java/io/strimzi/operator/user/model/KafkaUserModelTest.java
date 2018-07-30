/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.operator.common.operator.MockCertManager;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaUserModelTest {
    private static Map labels = Collections.singletonMap("foo", "bar");
    private static String namespace = "namespace";
    private static String name = "user";

    private static KafkaUser user;
    private static Secret clientsCa;
    private static Secret userCert;

    static {
        user = new KafkaUserBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                        .withNamespace(namespace)
                        .withName(name)
                        .withLabels(labels)
                        .build()
                )
                .withNewSpec()
                    .withAuthentication(new KafkaUserTlsClientAuthentication())
                .endSpec()
                .build();

        clientsCa = new SecretBuilder()
                .withNewMetadata()
                    .withName("somename")
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("clients-ca.key", Base64.getEncoder().encodeToString("clients-ca-key".getBytes()))
                .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .build();

        userCert = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Labels.userLabels(labels).withKind(KafkaUser.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .addToData("user.key", Base64.getEncoder().encodeToString("expected-key".getBytes()))
                .addToData("user.crt", Base64.getEncoder().encodeToString("expected-crt".getBytes()))
                .build();
    }

    @Test
    public void testFromCrd()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(new MockCertManager(), user, clientsCa, null);

        assertEquals(namespace, model.namespace);
        assertEquals(name, model.name);
        assertEquals(Labels.userLabels(labels).withKind(KafkaUser.RESOURCE_KIND), model.labels);
        assertEquals(KafkaUserTlsClientAuthentication.TYPE_TLS, model.authentication.getType());
    }

    @Test
    public void testGenerateSecret()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(new MockCertManager(), user, clientsCa, null);
        Secret generated = model.generateSecret();

        System.out.println(generated.getData().keySet());

        assertEquals(name, generated.getMetadata().getName());
        assertEquals(namespace, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(labels).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());
    }

    @Test
    public void testGenerateCertificateWhenNoExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(new MockCertManager(), user, clientsCa, null);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testGenerateCertificateAtCaChange()    {
        Secret clientsCa = new SecretBuilder()
                .withNewMetadata()
                .withName("somename")
                .withNamespace(namespace)
                .endMetadata()
                .addToData("clients-ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()))
                .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()))
                .build();

        KafkaUserModel model = KafkaUserModel.fromCrd(new MockCertManager(), user, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals("different-clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testGenerateCertificateKeepExisting()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(new MockCertManager(), user, clientsCa, userCert);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("expected-crt", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("expected-key", new String(model.decodeFromSecret(generated, "user.key")));
    }

    @Test
    public void testNoTlsAuth()    {
        KafkaUser user = new KafkaUserBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withNamespace(namespace)
                                .withName(name)
                                .withLabels(labels)
                                .build()
                )
                .withNewSpec()
                .endSpec()
                .build();
        KafkaUserModel model = KafkaUserModel.fromCrd(new MockCertManager(), user, clientsCa, userCert);

        assertNull(model.generateSecret());
    }
}

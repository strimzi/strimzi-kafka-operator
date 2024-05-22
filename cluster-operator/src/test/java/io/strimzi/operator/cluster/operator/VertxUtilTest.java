/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.api.kafka.model.common.GenericSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class VertxUtilTest {

    @Test
    void getHashOk() {
        String namespace = "ns";

        GenericSecretSource at = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-at")
                .withKey("key")
                .build();

        GenericSecretSource cs = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-cs")
                .withKey("key")
                .build();

        GenericSecretSource rt = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-rt")
                .withKey("key")
                .build();
        KafkaClientAuthentication kcu = new KafkaClientAuthenticationOAuthBuilder()
                .withAccessToken(at)
                .withRefreshToken(rt)
                .withClientSecret(cs)
                .build();

        CertSecretSource css = new CertSecretSourceBuilder()
                .withCertificate("key")
                .withSecretName("css-secret")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("key", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("top-secret-at"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-rt"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-cs"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("css-secret"))).thenReturn(Future.succeededFuture(secret));
        Future<Integer> res = VertxUtil.authTlsHash(secretOps, "ns", kcu, singletonList(css));
        res.onComplete(v -> {
            assertThat(v.succeeded(), is(true));
            // we are summing "value" hash four times
            assertThat(v.result(), is("value".hashCode() * 4));
        });
    }

    @Test
    void getHashFailure() {
        String namespace = "ns";

        GenericSecretSource at = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-at")
                .withKey("key")
                .build();

        GenericSecretSource cs = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-cs")
                .withKey("key")
                .build();

        GenericSecretSource rt = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-rt")
                .withKey("key")
                .build();
        KafkaClientAuthentication kcu = new KafkaClientAuthenticationOAuthBuilder()
                .withAccessToken(at)
                .withRefreshToken(rt)
                .withClientSecret(cs)
                .build();

        CertSecretSource css = new CertSecretSourceBuilder()
                .withCertificate("key")
                .withSecretName("css-secret")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("key", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("top-secret-at"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-rt"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-cs"))).thenReturn(Future.succeededFuture(null));
        when(secretOps.getAsync(eq(namespace), eq("css-secret"))).thenReturn(Future.succeededFuture(secret));
        Future<Integer> res = VertxUtil.authTlsHash(secretOps, "ns", kcu, singletonList(css));
        res.onComplete(v -> {
            assertThat(v.succeeded(), is(false));
            assertThat(v.cause().getMessage(), is("Secret top-secret-cs not found"));
        });
    }

    @Test
    void getHashForPattern(VertxTestContext context) {
        String namespace = "ns";

        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret")
                .withPattern("*.crt")
                .build();
        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret2")
                .withPattern("*.crt")
                .build();
        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret3")
                .withCertificate("my.crt")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("ca.crt", "value", "ca2.crt", "value2"))
                .build();
        Secret secret2 = new SecretBuilder()
                .withData(Map.of("ca3.crt", "value3", "ca4.crt", "value4"))
                .build();
        Secret secret3 = new SecretBuilder()
                .withData(Map.of("my.crt", "value5"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("cert-secret"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("cert-secret2"))).thenReturn(Future.succeededFuture(secret2));
        when(secretOps.getAsync(eq(namespace), eq("cert-secret3"))).thenReturn(Future.succeededFuture(secret3));

        Checkpoint async = context.checkpoint();
        VertxUtil.authTlsHash(secretOps, "ns", null, List.of(cert1, cert2, cert3)).onComplete(context.succeeding(res -> {
            assertThat(res, is("valuevalue2".hashCode() + "value3value4".hashCode() + "value5".hashCode()));
            async.flag();
        }));
    }

    @Test
    void getHashPatternNotMatching(VertxTestContext context) {
        String namespace = "ns";

        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret")
                .withPattern("*.pem")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("ca.crt", "value", "ca2.crt", "value2"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("cert-secret"))).thenReturn(Future.succeededFuture(secret));

        Checkpoint async = context.checkpoint();
        VertxUtil.authTlsHash(secretOps, "ns", null, singletonList(cert1)).onComplete(context.succeeding(res -> {
            assertThat(res, is(0));
            async.flag();
        }));
    }

    @Test
    void testAuthTlsHashScramSha512SecretFoundAndPasswordNotFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationScramSha512 auth = new KafkaClientAuthenticationScramSha512();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("password1");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = VertxUtil.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.failed());
            assertEquals("Items with key(s) [password1] are missing in Secret my-secret", handler.cause().getMessage());
        });
    }

    @Test
    void testAuthTlsHashScramSha512SecretAndPasswordFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationScramSha512 auth = new KafkaClientAuthenticationScramSha512();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("passwordKey");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = VertxUtil.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.succeeded());
            assertEquals("my-password".hashCode(), handler.result());
        });
    }

    @Test
    void testAuthTlsPlainSecretFoundAndPasswordNotFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationPlain auth = new KafkaClientAuthenticationPlain();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("password1");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = VertxUtil.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.failed());
            assertEquals("Items with key(s) [password1] are missing in Secret my-secret", handler.cause().getMessage());
        });
    }

    @Test
    void testAuthTlsPlainSecretAndPasswordFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationPlain auth = new KafkaClientAuthenticationPlain();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("passwordKey");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = VertxUtil.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.succeeded());
            assertEquals("my-password".hashCode(), handler.result());
        });
    }

    @Test
    void testGetValidateSecret() {
        String namespace = "ns";
        String secretName = "my-secret";

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of("key1", "value", "key2", "value", "key3", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq(secretName))).thenReturn(Future.succeededFuture(secret));

        VertxUtil.getValidatedSecret(secretOps, namespace, secretName, "key1", "key2")
                .onComplete(r -> {
                    assertThat(r.succeeded(), is(true));
                    assertThat(r.result(), is(secret));
                });
    }

    @Test
    void testGetValidateSecretMissingSecret() {
        String namespace = "ns";
        String secretName = "my-secret";

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq(secretName))).thenReturn(Future.succeededFuture(null));

        VertxUtil.getValidatedSecret(secretOps, namespace, secretName, "key1", "key2")
                .onComplete(r -> {
                    assertThat(r.succeeded(), is(false));
                    assertThat(r.cause().getMessage(), is("Secret my-secret not found in namespace ns"));
                });
    }

    @Test
    void testGetValidateSecretMissingKeys() {
        String namespace = "ns";
        String secretName = "my-secret";

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of("key1", "value", "key2", "value", "key3", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq(secretName))).thenReturn(Future.succeededFuture(secret));

        VertxUtil.getValidatedSecret(secretOps, namespace, secretName, "key1", "key4", "key5")
                .onComplete(r -> {
                    assertThat(r.succeeded(), is(false));
                    assertThat(r.cause().getMessage(), is("Items with key(s) [key4, key5] are missing in Secret my-secret"));
                });
    }

}

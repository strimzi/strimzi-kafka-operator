/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTls;
import io.strimzi.certs.CertAndKey;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.nio.charset.StandardCharsets;

/**
 * Operations for {@code Secret}s.
 */
public class SecretOperator extends AbstractResourceOperator<KubernetesClient, Secret, SecretList, Resource<Secret>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public SecretOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Secret");
    }

    @Override
    protected MixedOperation<Secret, SecretList, Resource<Secret>> operation() {
        return client.secrets();
    }

    public Future<String> getCertificateAsync(String namespace, CertSecretSource certSecretSource) {
        return this.getAsync(namespace, certSecretSource.getSecretName())
                .compose(secret -> secret == null ? Future.failedFuture("Secret " + certSecretSource.getSecretName() + " not found") : Future.succeededFuture(secret.getData().get(certSecretSource.getCertificate())));
    }

    public Future<CertAndKey> getCertificateAndKeyAsync(String namespace, KafkaClientAuthenticationTls auth) {
        return this.getAsync(namespace, auth.getCertificateAndKey().getSecretName())
                .compose(secret -> secret == null ? Future.failedFuture("Secret " + auth.getCertificateAndKey().getSecretName() + " not found") :
                        Future.succeededFuture(new CertAndKey(secret.getData().get(auth.getCertificateAndKey().getKey()).getBytes(StandardCharsets.UTF_8), secret.getData().get(auth.getCertificateAndKey().getCertificate()).getBytes(StandardCharsets.UTF_8))));
    }

    public Future<String> getPasswordAsync(String namespace, KafkaClientAuthentication auth) {
        if (auth instanceof KafkaClientAuthenticationPlain) {
            return this.getAsync(namespace, ((KafkaClientAuthenticationPlain) auth).getPasswordSecret().getSecretName())
                    .compose(secret -> secret == null ? Future.failedFuture("Secret " + ((KafkaClientAuthenticationPlain) auth).getPasswordSecret().getSecretName() + " not found") :
                            Future.succeededFuture(secret.getData().get(((KafkaClientAuthenticationPlain) auth).getPasswordSecret().getPassword())));
        }
        if (auth instanceof KafkaClientAuthenticationScramSha512) {
            return this.getAsync(namespace, ((KafkaClientAuthenticationScramSha512) auth).getPasswordSecret().getSecretName())
                    .compose(secret -> secret == null ? Future.failedFuture("Secret " + ((KafkaClientAuthenticationScramSha512) auth).getPasswordSecret().getSecretName() + " not found") :
                            Future.succeededFuture(secret.getData().get(((KafkaClientAuthenticationScramSha512) auth).getPasswordSecret().getPassword())));
        } else {
            return Future.failedFuture("Auth type " + auth.getType() + " does not have a password property");
        }
    }
}

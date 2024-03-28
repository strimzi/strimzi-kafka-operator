/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.common.KafkaFuture;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Class with various utility methods that use or depend on Vert.x core.
 */
public final class VertxUtil {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(VertxUtil.class);

    private VertxUtil() {
        // Not used
    }

    /**
     * Executes blocking code asynchronously
     *
     * @param vertx     Vert.x instance
     * @param supplier  Supplier with the blocking code
     *
     * @return  Future for returning the result
     *
     * @param <T>   Type of the result
     */
    public static <T> Future<T> async(Vertx vertx, Supplier<T> supplier) {
        return vertx.executeBlocking(supplier::get);
    }

    /**
     * Invoke the given {@code completed} supplier on a pooled thread approximately every {@code pollIntervalMs}
     * milliseconds until it returns true or {@code timeoutMs} milliseconds have elapsed.
     * @param reconciliation The reconciliation
     * @param vertx The vertx instance.
     * @param logContext A string used for context in logging.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param completed Determines when the wait is complete by returning true.
     * @return A future that completes when the given {@code completed} indicates readiness.
     */
    public static Future<Void> waitFor(Reconciliation reconciliation, Vertx vertx, String logContext, String logState, long pollIntervalMs, long timeoutMs, BooleanSupplier completed) {
        return waitFor(reconciliation, vertx, logContext, logState, pollIntervalMs, timeoutMs, completed, error -> false);
    }

    /**
     * Invoke the given {@code completed} supplier on a pooled thread approximately every {@code pollIntervalMs}
     * milliseconds until it returns true or {@code timeoutMs} milliseconds have elapsed.
     * @param reconciliation The reconciliation
     * @param vertx The vertx instance.
     * @param logContext A string used for context in logging.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param completed Determines when the wait is complete by returning true.
     * @param failOnError Determine whether a given error thrown by {@code completed},
     *                    should result in the immediate completion of the returned Future.
     * @return A future that completes when the given {@code completed} indicates readiness.
     */
    public static Future<Void> waitFor(Reconciliation reconciliation, Vertx vertx, String logContext, String logState, long pollIntervalMs, long timeoutMs, BooleanSupplier completed,
                                       Predicate<Throwable> failOnError) {
        Promise<Void> promise = Promise.promise();
        LOGGER.debugCr(reconciliation, "Waiting for {} to get {}", logContext, logState);
        long deadline = System.currentTimeMillis() + timeoutMs;
        Handler<Long> handler = new Handler<>() {
            @Override
            public void handle(Long timerId) {
                vertx.createSharedWorkerExecutor("kubernetes-ops-pool")
                        .executeBlocking(() -> {
                            boolean result;

                            try {
                                result = completed.getAsBoolean();
                            } catch (Throwable e) {
                                LOGGER.warnCr(reconciliation, "Caught exception while waiting for {} to get {}", logContext, logState, e);
                                throw e;
                            }

                            if (result) {
                                return null;
                            } else {
                                LOGGER.traceCr(reconciliation, "{} is not {}", logContext, logState);
                                throw new RuntimeException("Not " + logState + " yet");
                            }
                        })
                        .onComplete(res -> {
                            if (res.succeeded()) {
                                LOGGER.debugCr(reconciliation, "{} is {}", logContext, logState);
                                promise.complete();
                            } else {
                                if (failOnError.test(res.cause())) {
                                    promise.fail(res.cause());
                                } else {
                                    long timeLeft = deadline - System.currentTimeMillis();
                                    if (timeLeft <= 0) {
                                        String exceptionMessage = String.format("Exceeded timeout of %dms while waiting for %s to be %s", timeoutMs, logContext, logState);
                                        LOGGER.errorCr(reconciliation, exceptionMessage);
                                        promise.fail(new TimeoutException(exceptionMessage));
                                    } else {
                                        // Schedule ourselves to run again
                                        vertx.setTimer(Math.min(pollIntervalMs, timeLeft), this);
                                    }
                                }
                            }
                        });
            }
        };

        // Call the handler ourselves the first time
        handler.handle(null);

        return promise.future();
    }

    /**
     * Converts Kafka Future to Vert.x future
     *
     * @param reconciliation    Reconciliation marker
     * @param vertx             Vert.x instance
     * @param kf                Kafka future
     *
     * @return  Vert.x future based on the Kafka future
     *
     * @param <T>   Return type of the future
     */
    public static <T> Future<T> kafkaFutureToVertxFuture(Reconciliation reconciliation, Vertx vertx, KafkaFuture<T> kf) {
        Promise<T> promise = Promise.promise();
        if (kf != null) {
            kf.whenComplete((result, error) -> vertx.runOnContext(ignored -> {
                if (error != null) {
                    promise.fail(error);
                } else {
                    promise.complete(result);
                }
            }));
            return promise.future();
        } else {
            if (reconciliation != null) {
                LOGGER.traceCr(reconciliation, "KafkaFuture is null");
            } else {
                LOGGER.traceOp("KafkaFuture is null");
            }

            return Future.succeededFuture();
        }
    }

    /**
     * When TLS certificate or Auth certificate (or password) is changed, the hash is computed.
     * It is used for rolling updates.
     * @param secretOperations Secret operator
     * @param namespace namespace to get Secrets in
     * @param auth Authentication object to compute hash from
     * @param certSecretSources TLS trusted certificates whose hashes are joined to result
     * @return Future computing hash from TLS + Auth
     */
    public static Future<Integer> authTlsHash(SecretOperator secretOperations, String namespace, KafkaClientAuthentication auth, List<CertSecretSource> certSecretSources) {
        Future<Integer> tlsFuture;
        if (certSecretSources == null || certSecretSources.isEmpty()) {
            tlsFuture = Future.succeededFuture(0);
        } else {
            // get all TLS trusted certs, compute hash from each of them, sum hashes
            tlsFuture = Future.join(certSecretSources.stream().map(certSecretSource ->
                    getCertificateAsync(secretOperations, namespace, certSecretSource)
                    .compose(cert -> Future.succeededFuture(cert.hashCode()))).collect(Collectors.toList()))
                .compose(hashes -> Future.succeededFuture(hashes.list().stream().mapToInt(e -> (int) e).sum()));
        }

        if (auth == null) {
            return tlsFuture;
        } else {
            // compute hash from Auth
            if (auth instanceof KafkaClientAuthenticationScram) {
                // only passwordSecret can be changed
                return tlsFuture.compose(tlsHash -> getPasswordAsync(secretOperations, namespace, auth)
                        .compose(password -> Future.succeededFuture(password.hashCode() + tlsHash)));
            } else if (auth instanceof KafkaClientAuthenticationPlain) {
                // only passwordSecret can be changed
                return tlsFuture.compose(tlsHash -> getPasswordAsync(secretOperations, namespace, auth)
                        .compose(password -> Future.succeededFuture(password.hashCode() + tlsHash)));
            } else if (auth instanceof KafkaClientAuthenticationTls) {
                // custom cert can be used (and changed)
                return ((KafkaClientAuthenticationTls) auth).getCertificateAndKey() == null ? tlsFuture :
                        tlsFuture.compose(tlsHash -> getCertificateAndKeyAsync(secretOperations, namespace, (KafkaClientAuthenticationTls) auth)
                        .compose(crtAndKey -> Future.succeededFuture(crtAndKey.certAsBase64String().hashCode() + crtAndKey.keyAsBase64String().hashCode() + tlsHash)));
            } else if (auth instanceof KafkaClientAuthenticationOAuth) {
                List<Future<Integer>> futureList = ((KafkaClientAuthenticationOAuth) auth).getTlsTrustedCertificates() == null ?
                        new ArrayList<>() : ((KafkaClientAuthenticationOAuth) auth).getTlsTrustedCertificates().stream().map(certSecretSource ->
                        getCertificateAsync(secretOperations, namespace, certSecretSource)
                                .compose(cert -> Future.succeededFuture(cert.hashCode()))).collect(Collectors.toList());
                futureList.add(tlsFuture);
                futureList.add(addSecretHash(secretOperations, namespace, ((KafkaClientAuthenticationOAuth) auth).getAccessToken()));
                futureList.add(addSecretHash(secretOperations, namespace, ((KafkaClientAuthenticationOAuth) auth).getClientSecret()));
                futureList.add(addSecretHash(secretOperations, namespace, ((KafkaClientAuthenticationOAuth) auth).getRefreshToken()));
                return Future.join(futureList)
                        .compose(hashes -> Future.succeededFuture(hashes.list().stream().mapToInt(e -> (int) e).sum()));
            } else {
                // unknown Auth type
                return tlsFuture;
            }
        }
    }

    private static Future<Integer> addSecretHash(SecretOperator secretOperations, String namespace, GenericSecretSource genericSecretSource) {
        if (genericSecretSource != null) {
            return secretOperations.getAsync(namespace, genericSecretSource.getSecretName())
                    .compose(secret -> {
                        if (secret == null) {
                            return Future.failedFuture("Secret " + genericSecretSource.getSecretName() + " not found");
                        } else {
                            return Future.succeededFuture(secret.getData().get(genericSecretSource.getKey()).hashCode());
                        }
                    });
        }
        return Future.succeededFuture(0);
    }

    /**
     * Utility method which gets the secret and validates that the required fields are present in it
     *
     * @param secretOperator    Secret operator to get the secret from the Kubernetes API
     * @param namespace         Namespace where the Secret exist
     * @param name              Name of the Secret
     * @param items             List of items which should be present in the Secret
     *
     * @return      Future with the Secret if is exits and has the required items. Failed future with an error message otherwise.
     */
    /* test */ static Future<Secret> getValidatedSecret(SecretOperator secretOperator, String namespace, String name, String... items) {
        return secretOperator.getAsync(namespace, name)
                .compose(secret -> {
                    if (secret == null) {
                        return Future.failedFuture(new InvalidConfigurationException("Secret " + name + " not found"));
                    } else {
                        List<String> errors = new ArrayList<>(0);

                        for (String item : items)   {
                            if (!secret.getData().containsKey(item))    {
                                // Item not found => error will be raised
                                errors.add(item);
                            }
                        }

                        if (errors.isEmpty()) {
                            return Future.succeededFuture(secret);
                        } else {
                            return Future.failedFuture(new InvalidConfigurationException(String.format("Items with key(s) %s are missing in Secret %s", errors, name)));
                        }
                    }
                });
    }

    private static Future<String> getCertificateAsync(SecretOperator secretOperator, String namespace, CertSecretSource certSecretSource) {
        return getValidatedSecret(secretOperator, namespace, certSecretSource.getSecretName(), certSecretSource.getCertificate())
                .compose(secret -> Future.succeededFuture(secret.getData().get(certSecretSource.getCertificate())));
    }

    private static Future<CertAndKey> getCertificateAndKeyAsync(SecretOperator secretOperator, String namespace, KafkaClientAuthenticationTls auth) {
        return getValidatedSecret(secretOperator, namespace, auth.getCertificateAndKey().getSecretName(), auth.getCertificateAndKey().getCertificate(), auth.getCertificateAndKey().getKey())
                .compose(secret -> Future.succeededFuture(new CertAndKey(secret.getData().get(auth.getCertificateAndKey().getKey()).getBytes(StandardCharsets.UTF_8), secret.getData().get(auth.getCertificateAndKey().getCertificate()).getBytes(StandardCharsets.UTF_8))));
    }

    private static Future<String> getPasswordAsync(SecretOperator secretOperator, String namespace, KafkaClientAuthentication auth) {
        if (auth instanceof KafkaClientAuthenticationPlain plainAuth) {

            return getValidatedSecret(secretOperator, namespace, plainAuth.getPasswordSecret().getSecretName(), plainAuth.getPasswordSecret().getPassword())
                    .compose(secret -> Future.succeededFuture(secret.getData().get(plainAuth.getPasswordSecret().getPassword())));
        } else if (auth instanceof KafkaClientAuthenticationScram scramAuth) {

            return getValidatedSecret(secretOperator, namespace, scramAuth.getPasswordSecret().getSecretName(), scramAuth.getPasswordSecret().getPassword())
                    .compose(secret -> Future.succeededFuture(secret.getData().get(scramAuth.getPasswordSecret().getPassword())));
        } else {
            return Future.failedFuture("Auth type " + auth.getType() + " does not have a password property");
        }
    }

}

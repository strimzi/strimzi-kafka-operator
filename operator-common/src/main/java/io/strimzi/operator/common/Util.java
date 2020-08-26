/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Util {
    private static final Logger LOGGER = LogManager.getLogger(Util.class);

    public static <T> Future<T> async(Vertx vertx, Supplier<T> supplier) {
        Promise<T> result = Promise.promise();
        vertx.executeBlocking(
            future -> {
                try {
                    future.complete(supplier.get());
                } catch (Throwable t) {
                    future.fail(t);
                }
            }, result
        );
        return result.future();
    }

    /**
     * Invoke the given {@code completed} supplier on a pooled thread approximately every {@code pollIntervalMs}
     * milliseconds until it returns true or {@code timeoutMs} milliseconds have elapsed.
     * @param vertx The vertx instance.
     * @param logContext A string used for context in logging.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param completed Determines when the wait is complete by returning true.
     * @return A future that completes when the given {@code completed} indicates readiness.
     */
    public static Future<Void> waitFor(Vertx vertx, String logContext, String logState, long pollIntervalMs, long timeoutMs, BooleanSupplier completed) {
        return waitFor(vertx, logContext, logState, pollIntervalMs, timeoutMs, completed, error -> false);
    }

    /**
     * Invoke the given {@code completed} supplier on a pooled thread approximately every {@code pollIntervalMs}
     * milliseconds until it returns true or {@code timeoutMs} milliseconds have elapsed.
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
    public static Future<Void> waitFor(Vertx vertx, String logContext, String logState, long pollIntervalMs, long timeoutMs, BooleanSupplier completed,
                                       Predicate<Throwable> failOnError) {
        Promise<Void> promise = Promise.promise();
        LOGGER.debug("Waiting for {} to get {}", logContext, logState);
        long deadline = System.currentTimeMillis() + timeoutMs;
        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long timerId) {
                vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                    future -> {
                        try {
                            if (completed.getAsBoolean())   {
                                future.complete();
                            } else {
                                LOGGER.trace("{} is not {}", logContext, logState);
                                future.fail("Not " + logState + " yet");
                            }
                        } catch (Throwable e) {
                            LOGGER.warn("Caught exception while waiting for {} to get {}", logContext, logState, e);
                            future.fail(e);
                        }
                    },
                    true,
                    res -> {
                        if (res.succeeded()) {
                            LOGGER.debug("{} is {}", logContext, logState);
                            promise.complete();
                        } else {
                            if (failOnError.test(res.cause())) {
                                promise.fail(res.cause());
                            } else {
                                long timeLeft = deadline - System.currentTimeMillis();
                                if (timeLeft <= 0) {
                                    String exceptionMessage = String.format("Exceeded timeout of %dms while waiting for %s to be %s", timeoutMs, logContext, logState);
                                    LOGGER.error(exceptionMessage);
                                    promise.fail(new TimeoutException(exceptionMessage));
                                } else {
                                    // Schedule ourselves to run again
                                    vertx.setTimer(Math.min(pollIntervalMs, timeLeft), this);
                                }
                            }
                        }
                    }
                );
            }
        };

        // Call the handler ourselves the first time
        handler.handle(null);

        return promise.future();
    }

    /**
     * Parse a map from String.
     * For example a map of images {@code 2.0.0=strimzi/kafka:latest-kafka-2.0.0, 2.1.0=strimzi/kafka:latest-kafka-2.1.0}
     * or a map with labels / annotations {@code key1=value1 key2=value2}.
     *
     * @param str The string to parse.
     *
     * @return The parsed map.
     */
    public static Map<String, String> parseMap(String str) {
        if (str != null) {
            StringTokenizer tok = new StringTokenizer(str, ", \t\n\r");
            HashMap<String, String> map = new HashMap<>();
            while (tok.hasMoreTokens()) {
                String record = tok.nextToken();
                int endIndex = record.indexOf('=');

                if (endIndex == -1)  {
                    throw new RuntimeException("Failed to parse Map from String");
                }

                String key = record.substring(0, endIndex);
                String value = record.substring(endIndex + 1);
                map.put(key.trim(), value.trim());
            }
            return Collections.unmodifiableMap(map);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Returns exception when secret is missing. This is used from several different methods to provide identical exception.
     *
     * @param namespace     Namespace of the Secret
     * @param secretName    Name of the Secret
     * @return              RuntimeException
     */
    public static RuntimeException missingSecretException(String namespace, String secretName) {
        return new RuntimeException("Secret " + namespace + "/" + secretName + " does not exist");
    }

    /**
     * Create a file with Keystore or Truststore from the given {@code bytes}.
     * The file will be set to get deleted when the JVM exist.
     *
     * @param prefix    Prefix which will be used for the filename
     * @param suffix    Suffix which will be used for the filename
     * @param bytes     Byte array with the certificate store
     * @return          File with the certificate store
     */
    public static File createFileStore(String prefix, String suffix, byte[] bytes) {
        File f = null;
        try {
            f = File.createTempFile(prefix, suffix);
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                os.write(bytes);
            }
            return f;
        } catch (IOException e) {
            if (f != null && !f.delete()) {
                LOGGER.warn("Failed to delete temporary file in exception handler");
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode binary item from Kubernetes Secret from base64 into byte array
     *
     * @param secret    Kubernetes Secret
     * @param key       Key which should be retrived and decoded
     * @return          Decoded bytes
     */
    public static byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }

    /**
     * Create a Truststore file containing the given {@code certificate} and protected with {@code password}.
     * The file will be set to get deleted when the JVM exist.
     *
     * @param prefix Prefix which will be used for the filename
     * @param suffix Suffix which will be used for the filename
     * @param certificate X509 certificate to put inside the Truststore
     * @param password Password protecting the Truststore
     * @return File with the Truststore
     */
    public static File createFileTrustStore(String prefix, String suffix, X509Certificate certificate, char[] password) {
        try {
            KeyStore trustStore = null;
            trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, password);
            trustStore.setEntry(certificate.getSubjectDN().getName(), new KeyStore.TrustedCertificateEntry(certificate), null);
            return store(prefix, suffix, trustStore, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static File store(String prefix, String suffix, KeyStore trustStore, char[] password) throws Exception {
        File f = null;
        try {
            f = File.createTempFile(prefix, suffix);
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                trustStore.store(os, password);
            }
            return f;
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException | RuntimeException e) {
            if (f != null && !f.delete()) {
                LOGGER.warn("Failed to delete temporary file in exception handler");
            }
            throw e;
        }
    }

    /**
     * Logs environment variables into the regular log file.
     */
    public static void printEnvInfo() {
        Map<String, String> env = new HashMap<>(System.getenv());
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, String> entry: env.entrySet()) {
            sb.append("\t").append(entry.getKey()).append(": ").append(maskPassword(entry.getKey(), entry.getValue())).append("\n");
        }

        LOGGER.info("Using config:\n" + sb.toString());
    }

    /**
     * Gets environment variable, checks if it contains a password and in case it does it masks the output. It expects
     * environment variables with passwords to contain `PASSWORD` in their name.
     *
     * @param key   Name of the environment variable
     * @param value Value of the environment variable
     * @return      Value of the environment variable or masked text in case of password
     */
    public static String maskPassword(String key, String value)  {
        if (key.contains("PASSWORD"))  {
            return "********";
        } else {
            return value;
        }
    }

    /**
     * Merge two or more Maps together, should be used for merging multiple collections of Kubernetes labels or annotations
     *
     * @param base The base set of key value pairs that will be merged, if no overrides are present this will be returned.
     * @param overrides One or more Maps to merge with base, duplicate keys will be overwritten by last-in priority.
     *                  These are normally user configured labels/annotations that need to be merged with the base.
     *
     * @return A single Map of all the supplied maps merged together.
     */
    @SafeVarargs
    public static Map<String, String> mergeLabelsOrAnnotations(Map<String, String> base, Map<String, String>... overrides) {
        Map<String, String> merged = new HashMap<>();

        if (base != null) {
            merged.putAll(base);
        }

        if (overrides != null) {
            for (Map<String, String> toMerge : overrides) {

                if (toMerge == null) {
                    continue;
                }
                List<String> bannedLabelsOrAnnotations = toMerge
                    .keySet()
                    .stream()
                    .filter(key -> key.startsWith(Labels.STRIMZI_DOMAIN))
                    .collect(Collectors.toList());
                if (bannedLabelsOrAnnotations.size() > 0) {
                    throw new InvalidResourceException("User provided labels or annotations includes a Strimzi annotation: " + bannedLabelsOrAnnotations.toString());
                }

                Map<String, String> filteredToMerge = toMerge
                    .entrySet()
                    .stream()
                    // Remove Kubernetes Domain specific labels
                    .filter(entryset -> !entryset.getKey().startsWith(Labels.KUBERNETES_DOMAIN))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                merged.putAll(filteredToMerge);
            }
        }

        return merged;
    }

    public static <T> Future<T> kafkaFutureToVertxFuture(Vertx vertx, KafkaFuture<T> kf) {
        Promise<T> promise = Promise.promise();
        if (kf != null) {
            kf.whenComplete((result, error) -> {
                vertx.runOnContext(ignored -> {
                    if (error != null) {
                        promise.fail(error);
                    } else {
                        promise.complete(result);
                    }
                });
            });
            return promise.future();
        } else {
            LOGGER.trace("KafkaFuture is null");
            return Future.succeededFuture();
        }
    }

    public static ConfigResource getBrokersConfig(int podId) {
        return Util.getBrokersConfig(Integer.toString(podId));
    }

    public static ConfigResource getBrokersLogging(int podId) {
        return Util.getBrokersLogging(Integer.toString(podId));
    }

    public static ConfigResource getBrokersConfig(String podId) {
        return new ConfigResource(ConfigResource.Type.BROKER, podId);
    }

    public static ConfigResource getBrokersLogging(String podId) {
        return new ConfigResource(ConfigResource.Type.BROKER_LOGGER, podId);
    }

    /**
     * Computes and returns a hash from the String
     * @param toBeHashed String to be hashed
     * @return hash
     */
    public static String stringHash(String toBeHashed)  {
        try {
            MessageDigest hashFunc = MessageDigest.getInstance("SHA");

            byte[] hash = hashFunc.digest(toBeHashed.getBytes(StandardCharsets.UTF_8));

            StringBuffer stringHash = new StringBuffer();

            for (int i = 0; i < hash.length; i++) {
                String hex = Integer.toHexString(0xff & hash[i]);
                if (hex.length() == 1) stringHash.append('0');
                stringHash.append(hex);
            }

            return stringHash.toString();
        } catch (NoSuchAlgorithmException e)    {
            throw new RuntimeException("Failed to create SHA MessageDigest instance");
        }
    }

    /**
     * Method parses all dynamically unchangeable entries from the logging configuration.
     * @param loggingConfiguration logging configuration to be parsed
     * @return String containing all unmodifiable entries.
     */
    public static String getLoggingDynamicallyUnmodifiableEntries(String loggingConfiguration) {
        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(loggingConfiguration);
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> entry: ops.asMap().entrySet()) {
            if (!entry.getKey().startsWith("log4j.logger.") && !entry.getKey().equals("log4j.rootLogger") && !entry.getKey().equals("monitorInterval")) {
                result.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return result.toString();
    }

}

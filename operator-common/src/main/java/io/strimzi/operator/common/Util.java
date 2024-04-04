/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import org.apache.kafka.common.config.ConfigResource;
import org.quartz.CronExpression;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Class with various utility methods
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class Util {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Util.class);

    /**
     * Length of a hash stub. One example usage is when generating an annotation with a certificate short thumbprint.
     */
    public static final int HASH_STUB_LENGTH = 8;

    /**
     * Wrap the given action with a BiConsumer that {@link #unwrap(Throwable)
     * unwrap}s the error, if any. This method is meant to wrap the action typically
     * passed to {@link CompletionStage#whenComplete(BiConsumer)}.
     *
     * @param <T>    type of the asynchronous result
     * @param action action to be wrapped, expecting the cause of any
     *               {@link CompletionException} as its second argument
     * @return a BiConsumer to delegate to the provided action
     */
    public static <T> BiConsumer<T, Throwable> unwrap(BiConsumer<T, Throwable> action) {
        return (result, error) -> action.accept(result, unwrap(error));
    }

    /**
     * Returns the cause when the given Throwable is a {@link CompletionException}.
     * Otherwise the error is returned unchanged.
     *
     * @param error any Throwable
     * @return the cause when error is a {@link CompletionException}, else the same
     *         Throwable
     */
    public static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException wrapped) {
            return wrapped.getCause();
        }
        return error;
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
            f = Files.createTempFile(prefix, suffix).toFile();
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                os.write(bytes);
            }
            return f;
        } catch (IOException e) {
            if (f != null && !f.delete()) {
                LOGGER.warnOp("Failed to delete temporary file in exception handler");
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode binary item from Kubernetes Secret from base64 into byte array
     *
     * @param secret    Kubernetes Secret
     * @param field     Field which should be retrieved and decoded
     * @return          Decoded bytes
     */
    public static byte[] decodeBase64FieldFromSecret(Secret secret, String field) {
        Objects.requireNonNull(secret);
        String data = secret.getData().get(field);
        if (data != null) {
            return Base64.getDecoder().decode(data);
        } else {
            throw new RuntimeException(String.format("The Secret %s/%s is missing the field %s",
                    secret.getMetadata().getNamespace(),
                    secret.getMetadata().getName(),
                    field));
        }
    }

    /**
     * Decode binary item from Kubernetes Secret from base64 into String
     *
     * @param secret    Kubernetes Secret
     * @param field     Field which should be retrieved and decoded
     * @return          Decoded String
     */
    public static String asciiFieldFromSecret(Secret secret, String field) {
        return fromAsciiBytes(decodeBase64FieldFromSecret(secret, field));
    }

    /**
     * Decode the private key in PEM format, from base64 to a byte array.
     * Before decoding it into byte array, it removes the PEM header and footer.
     * @param privateKey        Key which should be decoded
     * @return                  Decoded bytes
     */
    public static byte[] decodePemPrivateKey(String privateKey) {
        String decodedPrivateKey = privateKey
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");
        return Base64.getDecoder().decode(decodedPrivateKey);
    }

    /**
     * Create a Truststore file containing the given {@code certificate} and protected with {@code password}.
     * The file will be set to get deleted when the JVM exist.
     *
     * @param prefix Prefix which will be used for the filename
     * @param suffix Suffix which will be used for the filename
     * @param certificates X509 certificates to put inside the Truststore
     * @param password Password protecting the Truststore
     * @return File with the Truststore
     */
    public static File createFileTrustStore(String prefix, String suffix, Set<X509Certificate> certificates, char[] password) {
        try {
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, password);

            int aliasIndex = 0;
            for (X509Certificate certificate : certificates) {
                trustStore.setEntry(certificate.getSubjectX500Principal().getName() + "-" + aliasIndex, new KeyStore.TrustedCertificateEntry(certificate), null);
                aliasIndex++;
            }

            return store(prefix, suffix, trustStore, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static File store(String prefix, String suffix, KeyStore trustStore, char[] password) throws Exception {
        File f = null;
        try {
            f = Files.createTempFile(prefix, suffix).toFile();
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                trustStore.store(os, password);
            }
            return f;
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException | RuntimeException e) {
            if (f != null && !f.delete()) {
                LOGGER.warnOp("Failed to delete temporary file in exception handler");
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

        LOGGER.infoOp("Using config:\n" + sb);
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
                    .toList();
                if (bannedLabelsOrAnnotations.size() > 0) {
                    throw new InvalidResourceException("User provided labels or annotations includes a Strimzi annotation: " + bannedLabelsOrAnnotations);
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

    /**
     * Created config resource instance
     *
     * @param podId Pod ID
     *
     * @return  Config resource
     */
    public static ConfigResource getBrokersConfig(int podId) {
        return Util.getBrokersConfig(Integer.toString(podId));
    }

    /**
     * Created config resource instance
     *
     * @param podId Pod ID
     *
     * @return  Config resource
     */
    public static ConfigResource getBrokersLogging(int podId) {
        return Util.getBrokersLogging(Integer.toString(podId));
    }

    /**
     * Created config resource instance
     *
     * @param podId Pod ID
     *
     * @return  Config resource
     */
    public static ConfigResource getBrokersConfig(String podId) {
        return new ConfigResource(ConfigResource.Type.BROKER, podId);
    }

    /**
     * Created config resource instance
     *
     * @param podId Pod ID
     *
     * @return  Config resource
     */
    public static ConfigResource getBrokersLogging(String podId) {
        return new ConfigResource(ConfigResource.Type.BROKER_LOGGER, podId);
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
        for (Map.Entry<String, String> entry: new TreeMap<>(ops.asMap()).entrySet()) {
            if (entry.getKey().startsWith("log4j.appender.") && !entry.getKey().equals("monitorInterval")) {
                result.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return result.toString();
    }

    /**
     * Load the properties and expand any variables of format ${NAME} inside values with resolved values.
     * Variables are resolved by looking up the property names only within the loaded map.
     *
     * @param env Multiline properties file as String
     * @return Multiline properties file as String with variables resolved
     */
    public static String expandVars(String env) {
        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(env);
        Map<String, String> map = ops.asMap();
        StringBuilder resultBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry: map.entrySet()) {
            resultBuilder.append(entry.getKey() + "=" + expandVar(entry.getValue(), ops.asMap()) + "\n");
        }
        return resultBuilder.toString();
    }

    /**
     * Search for occurrences of ${NAME} in the 'value' parameter and replace them with
     * the value for the NAME key in the 'env' map.
     *
     * @param value String value possibly containing variables of format: ${NAME}
     * @param env Var map with name:value pairs
     * @return Input string with variable references resolved
     */
    public static String expandVar(String value, Map<String, String> env) {
        StringBuilder sb = new StringBuilder();
        int endIdx = -1;
        int startIdx;
        int prefixLen = "${".length();
        while ((startIdx = value.indexOf("${", endIdx + 1)) != -1) {
            sb.append(value, endIdx + 1, startIdx);
            endIdx = value.indexOf("}", startIdx + prefixLen);
            if (endIdx != -1) {
                String key = value.substring(startIdx + prefixLen, endIdx);
                String resolved = env.get(key);
                sb.append(resolved != null ? resolved : "");
            } else {
                break;
            }
        }
        sb.append(value.substring(endIdx + 1));
        return sb.toString();
    }

    /**
     * Gets the first 8 characters from a SHA-1 hash of the provided String
     *
     * @param   toBeHashed  String for which the hash will be returned
     *
     * @return              First 8 characters of the SHA-1 hash
     */
    public static String hashStub(String toBeHashed)   {
        return hashStub(toBeHashed.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Gets the first 8 characters from a SHA-1 hash of the provided byte array
     *
     * @param   toBeHashed  Byte array for which the hash will be returned
     * @return              First 8 characters of the SHA-1 hash
     */
    public static String hashStub(byte[] toBeHashed)   {
        byte[] digest = sha1Digest(toBeHashed);
        return String.format("%040x", new BigInteger(1, digest)).substring(0, HASH_STUB_LENGTH);
    }

    /**
     * Get a SHA-1 hash of the provided byte array
     *
     * @param toBeHashed    Byte array for which the hash will be returned
     * @return              SHA-1 hash
     */
    public static byte[] sha1Digest(byte[] toBeHashed) {
        try {
            // This is used to generate unique identifier which is not used for security => using SHA-1 is ok
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return sha1.digest(toBeHashed);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get SHA-1 hash", e);
        }
    }

    /**
     * Checks if the Kubernetes resource matches LabelSelector. This is useful when you use get/getAsync to retrieve a
     * resource and want to check if it matches the labels from the selector (since get/getAsync is using name and not
     * labels to identify the resource). This method currently supports only the matchLabels object. matchExpressions
     * array is not supported.
     *
     * @param labelSelector The LabelSelector with the labels which should be present in the resource
     * @param cr            The Custom Resource which labels should be checked
     *
     * @return              True if the resource contains all labels from the LabelSelector or if the LabelSelector is empty
     */
    public static boolean matchesSelector(LabelSelector labelSelector, HasMetadata cr) {
        if (labelSelector != null && labelSelector.getMatchLabels() != null) {
            if (cr.getMetadata().getLabels() != null) {
                return cr.getMetadata().getLabels().entrySet().containsAll(labelSelector.getMatchLabels().entrySet());
            } else {
                return labelSelector.getMatchLabels().isEmpty();
            }
        }

        return true;
    }

    /**
     * Deleted a file from the filesystem
     *
     * @param key   Path to the file which should be deleted
     */
    public static void delete(Path key) {
        if (key != null) {
            try {
                Files.deleteIfExists(key);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Checks whether maintenance time window is satisfied by a given point in time or not
     *
     * @param reconciliation        Reconciliation marker
     * @param maintenanceWindows    List of maintenance windows
     * @param instant               The point in time to check the maintenance windows against
     *
     * @return                      True if we are in a maintenance window or if no maintenance windows are defined. False otherwise.
     */
    public static boolean isMaintenanceTimeWindowsSatisfied(Reconciliation reconciliation, List<String> maintenanceWindows, Instant instant) {
        String currentCron = null;
        try {
            boolean isSatisfiedBy = maintenanceWindows == null || maintenanceWindows.isEmpty();
            if (!isSatisfiedBy) {
                for (String cron : maintenanceWindows) {
                    currentCron = cron;
                    CronExpression cronExpression = new CronExpression(cron);
                    // the user defines the cron expression in "UTC/GMT" timezone but CO pod
                    // can be running on a different one, so setting it on the cron expression
                    cronExpression.setTimeZone(TimeZone.getTimeZone("GMT"));
                    if (cronExpression.isSatisfiedBy(Date.from(instant))) {
                        isSatisfiedBy = true;
                        break;
                    }
                }
            }
            return isSatisfiedBy;
        } catch (ParseException e) {
            LOGGER.warnCr(reconciliation, "The provided maintenance time windows list contains {} which is not a valid cron expression", currentCron);
            return false;
        }
    }

    /**
     * Encodes a String into Base64.
     *
     * @param data    String that should be encoded.
     *
     * @return        Base64 data.
     */
    public static String encodeToBase64(String data)  {
        return Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.US_ASCII));
    }
    
    /**
     * Decodes a byte[] from Base64.
     *
     * @param data    String that should be decoded.
     *
     * @return        Plain data in byte[].
     */
    public static byte[] decodeBytesFromBase64(String data)  {
        return Base64.getDecoder().decode(data);
    }
    
    /**
     * Decodes a byte[] from Base64.
     *
     * @param data    byte[] that should be decoded.
     *
     * @return        Plain data in byte[].
     */
    public static byte[] decodeBytesFromBase64(byte[] data)  {
        return Base64.getDecoder().decode(data);
    }

    /**
     * Decodes a String from Base64.
     *
     * @param data    String that should be decoded.
     *
     * @return        Plain data using US ASCII charset.
     */
    public static String decodeFromBase64(String data)  {
        return decodeFromBase64(data, StandardCharsets.US_ASCII);
    }
    
    /**
     * Decodes a String from Base64.
     *
     * @param data    String that should be decoded.
     * @param charset The charset for the return string
     *
     * @return        Plain data using specified charset.
     */
    public static String decodeFromBase64(String data, Charset charset)  {
        return new String(decodeBytesFromBase64(data), charset);
    }

    /**
     * Decodes the provided byte array using the charset StandardCharsets.US_ASCII
     * @param bytes Byte array to convert to String
     * @return New String object containing the provided byte array
     */
    public static String fromAsciiBytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.US_ASCII);
    }

}

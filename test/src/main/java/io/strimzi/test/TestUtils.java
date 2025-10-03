/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

/**
 * Class with various utility methods and fields useful for testing
 */
public final class TestUtils {
    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    /**
     * Path to the user directory from which the tests are run
     */
    public static final String USER_PATH = System.getProperty("user.dir");

    /**
     * The default line separator for the platform where the tests are run
     */
    public static final String LINE_SEPARATOR = System.lineSeparator();

    private TestUtils() {
        // All static methods
    }

    /**
     * Poll the given {@code ready} function every {@code pollIntervalMs} milliseconds until it returns true,
     * or throw a WaitException if it doesn't return true within {@code timeoutMs} milliseconds.
     *
     * @param description       Description of what we are waiting for (used for logging purposes)
     * @param pollIntervalMs    Poll interval in milliseconds
     * @param timeoutMs         Timeout interval in milliseconds
     * @param ready             Supplier to decide if the wait is complete or not
     *
     * @return  The remaining time left until timeout occurs (helpful if you have several calls which need to share a common timeout)
     */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        return waitFor(description, pollIntervalMs, timeoutMs, ready, () -> { });
    }

    /**
     * Poll the given {@code ready} function every {@code pollIntervalMs} milliseconds until it returns true,
     * or throw a WaitException if it doesn't return true within {@code timeoutMs} milliseconds.
     *
     * @param description       Description of what we are waiting for (used for logging purposes)
     * @param pollIntervalMs    Poll interval in milliseconds
     * @param timeoutMs         Timeout interval in milliseconds
     * @param ready             Supplier to decide if the wait is complete or not
     * @param onTimeout         Runnable that will be run when the timeout is reached
     *
     * @return  The remaining time left until timeout occurs (helpful if you have several calls which need to share a common timeout)
     */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready, Runnable onTimeout) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;

        String exceptionMessage = null;
        String previousExceptionMessage = null;

        // in case we are polling every 1s, we want to print exception after x tries, not on the first try
        // for minutes poll interval will 2 be enough
        int exceptionAppearanceCount = Duration.ofMillis(pollIntervalMs).toMinutes() > 0 ? 2 : Math.max((int) (timeoutMs / pollIntervalMs) / 4, 2);
        int exceptionCount = 0;
        int newExceptionAppearance = 0;

        StringWriter stackTraceError = new StringWriter();

        while (true) {
            boolean result;
            try {
                result = ready.getAsBoolean();
            } catch (Exception e) {
                exceptionMessage = e.getMessage();

                if (++exceptionCount == exceptionAppearanceCount && exceptionMessage != null && exceptionMessage.equals(previousExceptionMessage)) {
                    LOGGER.error("While waiting for {} exception occurred: {}", description, exceptionMessage);
                    // log the stacktrace
                    e.printStackTrace(new PrintWriter(stackTraceError));
                } else if (exceptionMessage != null && !exceptionMessage.equals(previousExceptionMessage) && ++newExceptionAppearance == 2) {
                    previousExceptionMessage = exceptionMessage;
                }

                result = false;
            }
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                if (exceptionCount > 1) {
                    LOGGER.error("Exception waiting for {}, {}", description, exceptionMessage);

                    if (!stackTraceError.toString().isEmpty()) {
                        // printing handled stacktrace
                        LOGGER.error(stackTraceError.toString());
                    }
                }
                onTimeout.run();
                throw new WaitException("Timeout after " + timeoutMs + " ms waiting for " + description);
            }
            long sleepTime = Math.min(pollIntervalMs, timeLeft);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} not ready, will try again in {} ms ({}ms till timeout)", description, sleepTime, timeLeft);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime));
        }
    }

    /**
     * Creates a modifiable set wit the desired elements. Use {@code Set.of()} if immutable set is sufficient.
     *
     * @param elements  The elements that will be added to the Set
     *
     * @return  Modifiable set with the elements
     *
     * @param <T>       Type of the elements stored in the Set
     */
    @SafeVarargs
    public static <T> Set<T> modifiableSet(T... elements) {
        return new HashSet<>(asList(elements));
    }

    /**
     * Creates a modifiable map wit the desired elements. Use {@code Map.of()} if immutable set is sufficient.
     *
     * @param pairs     The key-value pairs that should be added to the Map
     *
     * @return  Modifiable map with the desired key-value pairs
     *
     * @param <T>   Type of the keys and values
     */
    @SafeVarargs
    public static <T> Map<T, T> modifiableMap(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        } else {
            Map<T, T> result = new HashMap<>(pairs.length / 2);

            for (int i = 0; i < pairs.length; i += 2) {
                result.put(pairs[i], pairs[i + 1]);
            }

            return result;
        }
    }

    /**
     * Finds a free server port which can be used by the web server
     *
     * @return A free TCP port
     */
    public static int getFreePort()   {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find free port", e);
        }
    }

    /**
     * Awaits completion of the given stage using the default timeout.
     *
     * @param stage     The stage to await completion
     *
     * @return  Result of the completion stage
     *
     * @param <T>   Type of the completion stage result
     */
    public static <T> T await(CompletionStage<T> stage) {
        return await(stage, 30, TimeUnit.SECONDS);
    }

    /**
     * Awaits completion of the given stage using the given timeout and unit.
     *
     * @param stage     The stage to await completion
     * @param timeout   The amount of time to wait for completion
     * @param unit      The unit of time give by the timeout parameter
     *
     * @return  Result of the completion stage
     *
     * @param <T>   Type of the completion stage result
     */
    public static <T> T await(CompletionStage<T> stage, long timeout, TimeUnit unit) {
        try {
            return stage.toCompletableFuture().get(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting for CompletionStage", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("CompletionStage failed to complete", e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("CompletionStage timed out", e);
        }
    }

    /**
     * Asserts that the given error is null, indicating that the result of an
     * asynchronous execution stage completed successfully. This method is meant to
     * be used with
     * {@link CompletionStage#whenComplete(java.util.function.BiConsumer)
     * CompletionStage#whenComplete} to easily assert success without modifying the
     * result.
     *
     * @param unused    The result of a completion stage, unused by this method
     * @param error     An error thrown by an earlier completion stage
     */
    @SuppressWarnings("unused")
    public static void assertSuccessful(Object unused, Throwable error) {
        assertThat(error, is(nullValue()));
    }

    /**
     * Creates the namespace. If the namespace already exists, it will delete it and recreate it.
     *
     * @param client        Kubernetes client
     * @param namespace     Namespace
     */
    public static void createNamespace(KubernetesClient client, String namespace)   {
        if (client.namespaces().withName(namespace).get() != null) {
            LOGGER.warn("Namespace {} is already created, going to delete it and recreate it", namespace);
            deleteNamespace(client, namespace);
        }

        LOGGER.info("Creating namespace: {}", namespace);
        client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build()).create();
        client.namespaces().withName(namespace).waitUntilCondition(ns -> ns.getStatus() != null && "Active".equals(ns.getStatus().getPhase()), 30_000, TimeUnit.MILLISECONDS);
    }

    /**
     * Deletes the namespace if it exists (unless the SKIP_TEARDOWN environment variable is set)
     *
     * @param client        Kubernetes client
     * @param namespace     Namespace
     */
    public static void deleteNamespace(KubernetesClient client, String namespace)   {
        if (client.namespaces().withName(namespace).get() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Deleting namespace {} after tests run", namespace);
            client.namespaces().withName(namespace).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            client.namespaces().withName(namespace).waitUntilCondition(Objects::isNull, 30_000, TimeUnit.MILLISECONDS);
        }
    }
}

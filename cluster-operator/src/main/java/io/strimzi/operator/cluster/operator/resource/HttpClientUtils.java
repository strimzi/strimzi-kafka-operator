/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;

import java.util.function.BiConsumer;

/**
 * Utils for creating Http client and working with them
 */
public class HttpClientUtils {
    /**
     * Perform the given operation, which completes the promise, using an HTTP client instance,
     * after which the client is closed and the future for the promise returned.
     * @param vertx The vertx instance.
     * @param options Any client options that should be applied.
     * @param operation The operation to perform.
     * @param <T> The type of the result
     * @return A future which is completed with the result performed by the operation
     */
    public static <T> Future<T> withHttpClient(Vertx vertx, HttpClientOptions options, BiConsumer<HttpClient, Promise<T>> operation) {
        HttpClient httpClient = vertx.createHttpClient(options);
        Promise<T> promise = Promise.promise();
        operation.accept(httpClient, promise);
        return promise.future().compose(
            result -> {
                httpClient.close();
                return Future.succeededFuture(result);
            },
            error -> {
                httpClient.close();
                return Future.failedFuture(error);
            });
    }
}

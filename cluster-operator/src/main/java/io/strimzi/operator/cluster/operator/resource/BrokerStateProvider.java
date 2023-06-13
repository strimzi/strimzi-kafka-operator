/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

public class BrokerStateProvider {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BrokerStateProvider.class.getName());

    protected String namespace;
    private static final String BROKER_STATE_REST_PATH = "/v1/broker-state";
    private static final int BROKER_STATE_HTTPS_PORT = 8443;
    private static final String CLUSTER_CA_CERT_SECRET_KEY = "ca.crt";
    private static final String CO_KEY_SECRET_KEY = "cluster-operator.key";
    private static final String CO_KEY_CERT_SECRET_KEY = "cluster-operator.crt";

    private Vertx vertx;
    private Reconciliation reconciliation;
    private String cluster;
    private PemTrustOptions pto;
    private PemKeyCertOptions pkco;

    BrokerStateProvider(Reconciliation reconciliation, Vertx vertx, String cluster, String namespace, Secret clusterCaCertSecret, Secret coKeySecret) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.cluster = cluster;
        this.namespace = namespace;
        if (clusterCaCertSecret != null && clusterCaCertSecret.getData().containsKey(CLUSTER_CA_CERT_SECRET_KEY)) {
            this.pto = new PemTrustOptions().addCertValue(Buffer.buffer(Util.decodeFromSecret(clusterCaCertSecret, CLUSTER_CA_CERT_SECRET_KEY)));
        }
        if  (coKeySecret != null && coKeySecret.getData().containsKey(CO_KEY_SECRET_KEY) && coKeySecret.getData().containsKey(CO_KEY_CERT_SECRET_KEY)) {
            this.pkco = new PemKeyCertOptions()
                    .addCertValue(Buffer.buffer(Util.decodeFromSecret(coKeySecret, CO_KEY_CERT_SECRET_KEY)))
                    .addKeyValue(Buffer.buffer(Util.decodeFromSecret(coKeySecret, CO_KEY_SECRET_KEY)));
        }
    }

    private HttpClientOptions getHttpClientOptions() {
        return new HttpClientOptions()
                .setLogActivity(true)
                .setSsl(true)
                .setVerifyHost(true)
                .setPemTrustOptions(pto)
                .setKeyCertOptions(pkco);
    }

    protected <T> Future<String> doGet(String host) {
        LOGGER.debugCr(reconciliation, "Making GET request to {}", BROKER_STATE_REST_PATH);
        return HttpClientUtils.withHttpClient(vertx, getHttpClientOptions(), (httpClient, result) ->
                httpClient.request(HttpMethod.GET, BROKER_STATE_HTTPS_PORT, host, BROKER_STATE_REST_PATH, request -> {
                    if (request.succeeded()) {
                        request.result().setFollowRedirects(true)
                        .putHeader("Accept", "application/json");
                        request.result().send(response -> {
                            if (response.succeeded()) {
                                if (response.result().statusCode() == 200) {
                                    response.result().bodyHandler(body -> {
                                        var jsonBody = body.toString();
                                        result.complete(jsonBody);
                                    });
                                } else {
                                    result.fail(new RuntimeException("Unexpected HTTP status code: " + response.result().statusCode()));
                                }
                            } else {
                                result.tryFail(response.cause());
                            }
                        });
                    } else {
                        result.tryFail(request.cause());
                    }
                }));
    }

    public BrokerState getBrokerState(String podName) {
        BrokerState brokerstate = new BrokerState(-1, null);
        String host = DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName);
        CompletableFuture<String> completableFuture = new CompletableFuture<>();

        Future<String> getFuture = doGet(host);
        getFuture.onComplete(future -> {
            if (future.succeeded()) {
                completableFuture.complete(future.result());
            } else {
                completableFuture.completeExceptionally(future.cause());
            }
        });

        try {
            var result = completableFuture.get(3, TimeUnit.SECONDS);
            ObjectMapper mapper = new ObjectMapper();
            brokerstate = mapper.readValue(result, BrokerState.class);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            LOGGER.warnCr(reconciliation, "Failed to get broker state", e);
        } catch (JsonProcessingException jsonException) {
            LOGGER.warnCr(reconciliation, "Failed to parse response for broker state", jsonException);
        }
        return brokerstate;
    }
}

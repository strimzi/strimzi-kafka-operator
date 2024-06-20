/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlHeaders;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.strimzi.operator.topic.TopicOperatorUtil;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.CruiseControlUtil.buildBasicAuthValue;
import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.logging.log4j.core.util.Throwables.getRootCause;

/**
 * Cruise Control REST API client based on Java HTTP client.
 */
public class CruiseControlClientImpl implements CruiseControlClient {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CruiseControlClientImpl.class);
    
    private final String serverHostname;
    private final int serverPort;
    private final boolean rackEnabled;
    private final boolean sslEnabled;
    private final byte[] sslCertificate;
    private final boolean authEnabled;
    private final String authUsername;
    private final String authPassword;
    
    private final ExecutorService httpClientExecutor;
    private HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    CruiseControlClientImpl(String serverHostname,
                            int serverPort,
                            boolean rackEnabled,
                            boolean sslEnabled,
                            byte[] sslCertificate,
                            boolean authEnabled,
                            String authUsername,
                            String authPassword) {
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;
        this.rackEnabled = rackEnabled;
        this.sslEnabled = sslEnabled;
        this.sslCertificate = sslCertificate;
        this.authEnabled = authEnabled;
        this.authUsername = authUsername;
        this.authPassword = authPassword;
        this.httpClientExecutor = Executors.newCachedThreadPool();
        this.httpClient = buildHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void close() throws Exception {
        stopExecutor(httpClientExecutor, 10_000);
        httpClient = null;
    }

    @Override
    public String topicConfiguration(List<KafkaTopic> kafkaTopics) {
        // compute payload
        Map<Integer, List<KafkaTopic>> topicsByReplicas = kafkaTopics.stream()
            .collect(groupingBy(kt -> kt.getSpec().getReplicas()));
        Map<Integer, String> requestPayload = new HashMap<>();
        topicsByReplicas.entrySet().forEach(es -> {
            int rf = es.getKey();
            List<String> targetNames = topicsByReplicas.get(rf)
                .stream().map(TopicOperatorUtil::topicName).collect(Collectors.toList());
            requestPayload.put(rf, String.join("|", targetNames));
        });
        String jsonPayload;
        try {
            jsonPayload = objectMapper.writeValueAsString(
                new ReplicationFactorChanges(new ReplicationFactor(requestPayload)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(format("Request serialization failed: %s", e.getMessage()));
        }
        
        // build request
        URI requestUri = new UrlBuilder(serverHostname, serverPort, CruiseControlEndpoints.TOPIC_CONFIGURATION, sslEnabled)
            .withParameter(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK, String.valueOf(!rackEnabled))
            .withParameter(CruiseControlParameters.DRY_RUN, "false")
            .withParameter(CruiseControlParameters.JSON, "true")
            .build();
        
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(requestUri)
            .timeout(Duration.of(HTTP_REQUEST_TIMEOUT_SEC, ChronoUnit.SECONDS))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload));
        if (authEnabled) {
            builder.header("Authorization", buildBasicAuthValue(authUsername, authPassword));
        }
        HttpRequest request = builder.build();
        LOGGER.traceOp("Request: {}", request);
        
        // send request and handle response
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
            LOGGER.traceOp("Response: {}, body: {}", response, response.body());
            if (response.statusCode() != 200) {
                Optional<String> error = errorMessage(response);
                throw new RuntimeException(error.isPresent() 
                    ? format("Request failed (%s), %s", response.statusCode(), error.get()) 
                    : format("Request failed (%s)", response.statusCode())
                );
            }
            return response.headers().firstValue(CruiseControlHeaders.USER_TASK_ID_HEADER).get();
        }).exceptionally(t -> {
            if (t.getCause() instanceof ConnectException) {
                throw new RuntimeException("Connection failed");
            } else {
                throw new RuntimeException(getRootCause(t).getMessage());
            }
        }).join();
    }

    @Override
    public UserTasksResponse userTasks(Set<String> userTaskIds) {
        // build request
        URI requestUrl = new UrlBuilder(serverHostname, serverPort, CruiseControlEndpoints.USER_TASKS, sslEnabled)
            .withParameter(CruiseControlParameters.USER_TASK_IDS, new ArrayList<>(userTaskIds))
            .withParameter(CruiseControlParameters.FETCH_COMPLETE, "false")
            .withParameter(CruiseControlParameters.JSON, "true")
            .build();
        
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(requestUrl)
            .timeout(Duration.of(HTTP_REQUEST_TIMEOUT_SEC, ChronoUnit.SECONDS))
            .GET();
        if (authEnabled) {
            builder.header("Authorization", buildBasicAuthValue(authUsername, authPassword));
        }
        HttpRequest request = builder.build();
        LOGGER.traceOp("Request: {}", request);
        
        // send request and handle response
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
            LOGGER.traceOp("Response: {}, body: {}", response, response.body());
            if (response.statusCode() != 200) {
                Optional<String> error = errorMessage(response);
                throw new RuntimeException(error.isPresent()
                    ? format("Request failed (%s), %s", response.statusCode(), error.get())
                    : format("Request failed (%s)", response.statusCode())
                );
            }

            try {
                return objectMapper.readValue(response.body(), UserTasksResponse.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(format("Response deserialization failed, %s", e.getMessage()));
            }
        }).exceptionally(t -> {
            if (t.getCause() instanceof ConnectException) {
                throw new RuntimeException("Connection failed");
            } else {
                throw new RuntimeException(getRootCause(t).getMessage());   
            }
        }).join();
    }

    public Optional<String> errorMessage(HttpResponse<String> response) {
        if (response != null) {
            if (response.statusCode() == 401) {
                return Optional.of("Authorization error");
            }
            if (response.body() != null && !response.body().isEmpty()) {
                try {
                    ErrorResponse errorResponse = objectMapper.readValue(response.body(), ErrorResponse.class);
                    if (errorResponse.errorMessage() != null) {
                        if (errorResponse.errorMessage().contains("NotEnoughValidWindowsException")) {
                            return Optional.of("Cluster model not ready");
                        } else if (errorResponse.errorMessage().contains("OngoingExecutionException")
                            || errorResponse.errorMessage().contains("stop_ongoing_execution")) {
                            return Optional.of("Another task is executing");
                        } else {
                            return Optional.of(errorResponse.errorMessage());
                        }
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(format("Error deserialization failed: %s", e.getMessage()));
                }
            }
        }
        return Optional.empty();
    }
    
    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    private HttpClient buildHttpClient() {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder().executor(httpClientExecutor);
            if (sslEnabled) {
                // load the certificate chain to be trusted
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                Certificate ca;
                try (var caInput = new ByteArrayInputStream(sslCertificate)) {
                    ca = cf.generateCertificate(caInput);
                }
                
                // create a P12 keystore containing our trusted chain
                KeyStore keyStore = KeyStore.getInstance("PKCS12");
                keyStore.load(null, null);
                keyStore.setCertificateEntry("ca", ca);
                
                // create a trust manager that trusts the chain in our keystore
                TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
                tmf.init(keyStore);
                
                // create an SSL context that uses our trust manager
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, tmf.getTrustManagers(), null);
                builder.sslContext(sslContext);
            }
            return builder.build();
        } catch (Throwable t) {
            throw new RuntimeException(format("HTTP client build failed: %s", t.getMessage()));
        }
    }
    
    private static void stopExecutor(ExecutorService executor, long timeoutMs) {
        if (executor == null || timeoutMs < 0) {
            return;
        }
        try {
            executor.shutdown();
            executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
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
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.ONGOING;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.buildBasicAuthValue;
import static io.strimzi.operator.topic.TopicOperatorUtil.getFileContent;
import static io.strimzi.operator.topic.TopicOperatorUtil.hasReplicasChange;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicNames;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;

/**
 * Replicas change handler that interacts with Cruise Control REST API.
 * <br/><br/>
 * The REST endpoints are {@code topic_configuration} to request replication factor changes and {@code user_tasks} 
 * to check the asynchronous execution result. Cruise Control runs one task execution at a time, additional requests 
 * are queued up to {@code max.active.user.tasks}.
 * <br/><br/>
 * At any given time, a KafkaTopic.spec.replicas change can be in one of the following states:
 * <br/>
 * <ul><li>Pending: Not in Cruise Control's task queue (not yet sent or request error).</li>
 * <li>Ongoing: In Cruise Control's task queue, but execution not started, or not completed.</li>
 * <li>Completed: Cruise Control's task execution completed (target replication factor reconciled).</li></ul>
 */
public class ReplicasChangeHandler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ReplicasChangeHandler.class);
    private static final String USER_TASK_ID_HEADER = "User-Task-ID";
    private static final long REQUEST_TIMEOUT_SEC = 60;
    
    private final TopicOperatorConfig config;
    private final ExecutorService httpClientExecutor;
    private final ObjectMapper mapper;
    
    /**
     * Create a new replicas change client instance.
     * 
     * @param config Topic Operator configuration.
     */
    public ReplicasChangeHandler(TopicOperatorConfig config) {
        this.config = config;
        this.httpClientExecutor = Executors.newCachedThreadPool();
        this.mapper = new ObjectMapper();
    }

    /**
     * Send a topic_configuration request to create a task for replication factor change of one or more topics.
     * This should be called when one or more .spec.replicas changes are detected.
     * Note that this method also updates the KafkaTopic status.
     * 
     * @param reconcilableTopics Pending replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestPendingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) {
            return result;
        }
        updateToPending(reconcilableTopics, "Replicas change pending");
        result.addAll(reconcilableTopics);
        
        try {
            LOGGER.debugOp("Sending topic configuration request, topics {}", topicNames(reconcilableTopics));
            HttpClient client = buildHttpClient();
            client.sendAsync(buildPostTopicConfigRequest(reconcilableTopics), HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() != 200) {
                        updateToFailed(result, format("Replicas change failed (%s)", response.statusCode()), response);
                        return;
                    }
                    try {
                        String userTaskId = response.headers().firstValue(USER_TASK_ID_HEADER).get();
                        updateToOngoing(result, "Replicas change ongoing", userTaskId);
                    } catch (Throwable e) {
                        updateToFailed(result, format("Failed to get %s header", USER_TASK_ID_HEADER), response);
                    }
                }).join();
        } catch (Throwable t) {
            updateToFailed(result, format("Replicas change failed, %s", t.getMessage()), null);
        }
        return result;
    }

    /**
     * Send a user_tasks request to check the state of ongoing replication factor changes.
     * This should be called periodically to update the active tasks cache and KafkaTopic status.
     * Note that this method also updates the KafkaTopic status.
     *
     * @param reconcilableTopics Ongoing replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestOngoingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) {
            return result;
        }
        result.addAll(reconcilableTopics);
        
        Map<String, List<ReconcilableTopic>> groupByUserTaskId = reconcilableTopics.stream()
            .filter(rt -> hasReplicasChange(rt.kt().getStatus()) && rt.kt().getStatus().getReplicasChange().getSessionId() != null)
            .map(rt -> new ReconcilableTopic(new Reconciliation("", KafkaTopic.RESOURCE_KIND, "", ""), rt.kt(), rt.topicName()))
            .collect(Collectors.groupingBy(rt -> rt.kt().getStatus().getReplicasChange().getSessionId(), HashMap::new, Collectors.toList()));

        try {
            LOGGER.debugOp("Sending user tasks request, Tasks {}", groupByUserTaskId.keySet());
            HttpClient client = buildHttpClient();
            client.sendAsync(buildGetUserTasksRequest(groupByUserTaskId.keySet()), HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    try {
                        if (response.statusCode() != 200 || response.body() == null) {
                            updateToFailed(result, format("Replicas change failed (%s)", response.statusCode()), response);
                            return;
                        }
                    
                        UserTasksResponse utr = mapper.readValue(response.body(), UserTasksResponse.class);
                        if (utr.userTasks().isEmpty()) {
                            // Cruise Control restarted: reset the state because the tasks queue is not persisted
                            updateToPending(result, "Task not found, Resetting the state");
                            return;
                        }
                        
                        for (var userTask : utr.userTasks()) {
                            String userTaskId = userTask.userTaskId();
                            TaskState state = TaskState.get(userTask.status());
                            switch (state) {
                                case COMPLETED:
                                    updateToCompleted(groupByUserTaskId.get(userTaskId), "Replicas change completed");
                                    break;
                                case COMPLETED_WITH_ERROR:
                                    updateToFailed(groupByUserTaskId.get(userTaskId), "Replicas change failed", response);
                                    break;
                                case ACTIVE:
                                case IN_EXECUTION:
                                    // do nothing
                                    break;
                            }
                        }
                    } catch (Throwable t) {
                        updateToFailed(result, format("Failed to parse response, %s", t.getMessage()), null);
                    }
                }).join();
        } catch (Throwable t) {
            updateToFailed(result, format("Replicas change failed, %s", t.getMessage()), null);
        }
        return result;
    }
    
    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    private HttpClient buildHttpClient() throws Exception {
        HttpClient.Builder builder = HttpClient.newBuilder().executor(httpClientExecutor);
        if (config.cruiseControlSslEnabled()) {
            // load the certificate chain to be trusted
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate ca;
            try (var caInput = new ByteArrayInputStream(getFileContent(config.cruiseControlCrtFilePath()))) {
                ca = cf.generateCertificate(caInput);
            } catch (IOException ioe) {
                throw new RuntimeException(format("File not found: %s", config.cruiseControlCrtFilePath()));
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
    }

    private HttpRequest buildPostTopicConfigRequest(List<ReconcilableTopic> reconcilableTopics) {
        StringBuilder url = new StringBuilder(
            format("%s://%s:%d%s?", config.cruiseControlSslEnabled() ? "https" : "http",
                config.cruiseControlHostname(), config.cruiseControlPort(), CruiseControlEndpoints.TOPIC_CONFIGURATION));
        url.append(format("%s=%s&", CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK, !config.cruiseControlRackEnabled()));
        url.append(format("%s=%s&", CruiseControlParameters.DRY_RUN, "false"));
        url.append(format("%s=%s", CruiseControlParameters.JSON, "true"));
        
        Map<Integer, List<ReconcilableTopic>> topicsByReplicas = reconcilableTopics.stream()
            .collect(groupingBy(rt -> rt.kt().getSpec().getReplicas()));
        Map<Integer, String> requestPayload = new HashMap<>();
        topicsByReplicas.entrySet().forEach(es -> {
            int rf = es.getKey();
            List<String> targetNames = topicNames(topicsByReplicas.get(rf));
            requestPayload.put(rf, String.join("|", targetNames));
        });
        String json;
        try {
            json = mapper.writeValueAsString(
                new ReplicationFactorChanges(new ReplicationFactor(requestPayload)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize request body");
        }
        
        LOGGER.traceOp("Request URL: {}, body: {}", url, json);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .timeout(Duration.of(REQUEST_TIMEOUT_SEC, ChronoUnit.SECONDS))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json));
        maybeAddBasicAuthHeader(builder);
        return builder.build();
    }
    
    private HttpRequest buildGetUserTasksRequest(Set<String> userTaskIds) {
        StringBuilder url = new StringBuilder(
            format("%s://%s:%d%s?", config.cruiseControlSslEnabled() ? "https" : "http",
                config.cruiseControlHostname(), config.cruiseControlPort(), CruiseControlEndpoints.USER_TASKS));
        url.append(format("%s=%s&", CruiseControlParameters.USER_TASK_IDS, URLEncoder.encode(String.join(",", userTaskIds), UTF_8)));
        url.append(format("%s=%s", CruiseControlParameters.JSON, "true"));
        
        LOGGER.traceOp("Request URL: {}", url);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .timeout(Duration.of(REQUEST_TIMEOUT_SEC, ChronoUnit.SECONDS))
            .GET();
        maybeAddBasicAuthHeader(builder);
        return builder.build();
    }

    private void maybeAddBasicAuthHeader(HttpRequest.Builder builder) {
        if (config.cruiseControlAuthEnabled()) {
            String apiUsername = new String(getFileContent(config.cruiseControlApiUserPath()), UTF_8);
            String apiPassword = new String(getFileContent(config.cruiseControlApiPassPath()), UTF_8);
            builder.header("Authorization", buildBasicAuthValue(apiUsername, apiPassword));
        }
    }

    private void updateToPending(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                    .withState(PENDING).withTargetReplicas(reconcilableTopic.kt().getSpec().getReplicas()).build()).build()));
    }
    
    private void updateToOngoing(List<ReconcilableTopic> reconcilableTopics, String message, String userTaskId) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withState(ONGOING).withSessionId(userTaskId).endReplicasChange().build()));
    }
    
    private void updateToCompleted(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(null).build()));
    }
    
    private void updateToFailed(List<ReconcilableTopic> reconcilableTopics, String message, HttpResponse<String> response) {
        Optional<String> error = parseErrorResponse(response);
        String text = error.isPresent() ? format("%s, %s", message, error.get()) : message;
        LOGGER.errorOp("{}, Topics: {}", text, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withMessage(text).endReplicasChange().build()));
    }

    private Optional<String> parseErrorResponse(HttpResponse<String> response) {
        if (response != null && response.body() != null) {
            try {
                ErrorResponse errorResponse = mapper.readValue(response.body(), ErrorResponse.class);
                if (errorResponse.errorMessage().contains("NotEnoughValidWindowsException")) {
                    return Optional.of("Cluster model not ready");
                } else if (errorResponse.errorMessage().contains("OngoingExecutionException")
                    || errorResponse.errorMessage().contains("stop_ongoing_execution")) {
                    return Optional.of("Another task is executing");
                } else {
                    return Optional.of(errorResponse.errorMessage());
                }
            } catch (Throwable t) {
                LOGGER.warnOp("Failed to parse error response: {}", t.getMessage());
            }
        }
        return Optional.empty();
    }

    private record ReplicationFactorChanges(@JsonProperty("replication_factor") ReplicationFactor replicationFactor) { }
    private record ReplicationFactor(@JsonProperty("topic_by_replication_factor") Map<Integer, String> topicByReplicationFactor) { }
    
    private record UserTasksResponse(List<UserTask> userTasks, int version) { }
    private record UserTask(
        @JsonProperty("Status") String status,
        @JsonProperty("ClientIdentity") String clientIdentity,
        @JsonProperty("RequestURL") String requestURL,
        @JsonProperty("UserTaskId") String userTaskId,
        @JsonProperty("StartMs") String startMs
    ) { }
    private record ErrorResponse(String stackTrace, String errorMessage, int version) { }
    
    private enum TaskState {
        ACTIVE("Active"),
        IN_EXECUTION("InExecution"),
        COMPLETED("Completed"),
        COMPLETED_WITH_ERROR("CompletedWithError");

        private static final List<TaskState> CACHED_VALUES = List.of(values());
        private final String value;
        TaskState(String value) {
            this.value = value;
        }
        
        @Override
        public String toString() {
            return value;
        }

        /**
         * Use this instead of values() to avoid creating a new array each time.
         * @return enumerated values in the same order as values()
         */
        public static List<TaskState> cachedValues() {
            return CACHED_VALUES;
        }

        /**
         * Get the enum constant by value.
         * @param value Value.
         * @return Constant.
         */
        public static TaskState get(String value) {
            Optional<TaskState> constant = cachedValues().stream()
                .filter(v -> v.toString().equals(value)).findFirst();
            return constant.orElseThrow();
        }
    }
}

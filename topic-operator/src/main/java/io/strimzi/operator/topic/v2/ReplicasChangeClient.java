/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

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
import java.io.File;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.SecureRandom;
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

import static io.strimzi.operator.topic.v2.TopicOperatorUtil.topicNames;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.stream.Collectors.groupingBy;

/**
 * Replicas change client for Cruise Control.
 * 
 * <br/><br/>
 * The REST endpoints used are {@code topic_configuration} to request replication factor changes and {@code user_tasks} 
 * to check the asynchronous execution result. Cruise Control runs one task execution at a time, additional requests 
 * are queued up to {@code max.active.user.tasks}.
 * 
 * <br/><br/>
 * At any given time, a .spec.replicas change can be in one of the following states:
 *
 * <ul><li>Pending: Not in Cruise Control's task queue (not yet sent or request error).</li>
 * <li>Ongoing: In Cruise Control's task queue, but execution not started, or not completed.</li>
 * <li>Completed: Cruise Control's task execution completed (target replication factor reconciled).</li></ul>
 */
public class ReplicasChangeClient {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ReplicasChangeClient.class);
    private static final String TRUSTSTORE_FILE = "/etc/tls-sidecar/cluster-ca-certs/ca.p12";
    private static final String TRUSTSTORE_PASS = "/etc/tls-sidecar/cluster-ca-certs/ca.password";
    private static final String API_USERNAME = "/etc/eto-cc-api/topic-operator.apiAdminName";
    private static final String API_PASSWORD = "/etc/eto-cc-api/topic-operator.apiAdminPassword";
    
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
    public ReplicasChangeClient(TopicOperatorConfig config) {
        this.config = config;
        this.httpClientExecutor = Executors.newCachedThreadPool();
        this.mapper = new ObjectMapper();
    }

    /**
     * Send a topic_configuration request to create a task for replication factor change of one or more topics.
     * This should be called when one ore more .spec.replicas changes are detected.
     * 
     * @param reconcilableTopics Pending replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestPendingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) return result;
        pending(reconcilableTopics, "Replicas change pending");
        result.addAll(reconcilableTopics);
        
        try {
            LOGGER.debugOp("Sending topic configuration request, topics {}", topicNames(reconcilableTopics));
            HttpClient client = buildCruiseControlApiClient();
            Map<Integer, List<ReconcilableTopic>> topicsByReplicas = reconcilableTopics.stream().collect(groupingBy(rt -> rt.kt().getSpec().getReplicas()));
            client.sendAsync(buildTopicConfiguartionPostRequest(topicsByReplicas), HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() != 200 || response.headers() == null) {
                        failed(reconcilableTopics, "Replicas change failed", response);
                        return;
                    }
                    try {
                        String userTaskId = response.headers().firstValue(USER_TASK_ID_HEADER).get();
                        ongoing(reconcilableTopics, "Replicas change ongoing", userTaskId);
                    } catch (Throwable e) {
                        failed(reconcilableTopics, "Failed to get task id header", response);
                    }
                }).join();
        } catch (Throwable e) {
            failed(reconcilableTopics, format("Replicas change failed: %s", e.getMessage()), null);
        }
        return result;
    }
    
    private HttpRequest buildTopicConfiguartionPostRequest(Map<Integer, List<ReconcilableTopic>> rtByReplicas) throws JsonProcessingException {
        StringBuilder url = new StringBuilder(
            format("%s://%s:%d%s?", config.cruiseControlSslEnabled() ? "https" : "http", 
            config.cruiseControlHostname(), config.cruiseControlPort(), CruiseControlEndpoints.TOPIC_CONFIGURATION));
        if (!config.cruiseControlRackEnabled()) {
            url.append(format("%s=%s&", CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK, "true"));
        }
        url.append(format("%s=%s&", CruiseControlParameters.DRY_RUN, "false"));
        url.append(format("%s=%s", CruiseControlParameters.JSON, "true"));
        Map<Integer, String> topicsByRf = new HashMap<>();
        rtByReplicas.entrySet().forEach(es -> {
            int rf = es.getKey();
            List<String> targetNames = topicNames(rtByReplicas.get(rf));
            topicsByRf.put(rf, String.join("|", targetNames));
        });
        String json = mapper.writeValueAsString(
            new ReplicationFactorChanges(new ReplicationFactor(topicsByRf)));
        LOGGER.traceOp("Request URL: {}, body: {}", url, json);
        return HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .timeout(Duration.of(REQUEST_TIMEOUT_SEC, ChronoUnit.SECONDS))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();
    }

    /**
     * Send a user_tasks request to check the state of ongoing replication factor changes.
     * This should be called periodically to update the active tasks cache and KafkaTopic status.
     *
     * @param reconcilableTopics Ongoing replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> checkOngoingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) return result;
        
        Map<String, List<ReconcilableTopic>> groupByUserTaskId = new HashMap<>();
        reconcilableTopics.forEach(rt -> {
            if (rt.kt().getStatus() != null && rt.kt().getStatus().getReplicasChange() != null && rt.kt().getStatus().getReplicasChange().getSessionId() != null) {
                String userTaskId = rt.kt().getStatus().getReplicasChange().getSessionId();
                ReconcilableTopic reconcilableTopic = new ReconcilableTopic(new Reconciliation("", KafkaTopic.RESOURCE_KIND, "", ""), rt.kt(), rt.topicName());
                groupByUserTaskId.computeIfAbsent(userTaskId, k -> new ArrayList<>()).add(reconcilableTopic);
            }
        });
        
        try {
            LOGGER.debugOp("Sending user tasks request, tasks {}", groupByUserTaskId.keySet());
            HttpClient client = buildCruiseControlApiClient();
            client.sendAsync(buildUserTasksGetRequest(groupByUserTaskId.keySet()), HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    try {
                        if (response.statusCode() != 200 || response.body() == null) {
                            LOGGER.errorOp("Ongoing check failed: {}", parseError(response));
                            return;
                        }
                    
                        UserTasksResponse utr = mapper.readValue(response.body(), UserTasksResponse.class);
                        if (utr.userTasks().isEmpty()) {
                            // Cruise Control restarted: reset the state because the tasks queue is not persisted
                            failed(reconcilableTopics, "Task not found: resetting the state", null);
                            return;
                        }
                        
                        for (var userTask : utr.userTasks()) {
                            String userTaskId = userTask.userTaskId();
                            TaskState state = TaskState.get(userTask.status());
                            switch (state) {
                                case COMPLETED:
                                    completed(groupByUserTaskId.get(userTaskId), "Replicas change completed");
                                    result.addAll(groupByUserTaskId.get(userTaskId));
                                    break;
                                case COMPLETED_WITH_ERROR:
                                    failed(groupByUserTaskId.get(userTaskId), "Replicas change failed", response);
                                    result.addAll(groupByUserTaskId.get(userTaskId));
                                    break;
                                case ACTIVE:
                                case IN_EXECUTION:
                                    // do nothing
                                    break;
                            }
                        }
                    } catch (Throwable e) {
                        LOGGER.errorOp("Failed to parse response {}: {}", response, e.getMessage());
                    }
                }).join();
        } catch (Throwable e) {
            LOGGER.errorOp("Ongoing check failed: {}", e.getMessage());
        }
        return result;
    }
    
    private HttpRequest buildUserTasksGetRequest(Set<String> userTaskIds) {
        StringBuilder url = new StringBuilder(
            format("%s://%s:%d%s?", config.cruiseControlSslEnabled() ? "https" : "http",
                config.cruiseControlHostname(), config.cruiseControlPort(), CruiseControlEndpoints.USER_TASKS));
        url.append(format("%s=%s&", CruiseControlParameters.USER_TASK_IDS, URLEncoder.encode(String.join(",", userTaskIds), US_ASCII)));
        url.append(format("%s=%s", CruiseControlParameters.JSON, "true"));
        LOGGER.traceOp("Request URL: {}", url);
        return HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .timeout(Duration.of(REQUEST_TIMEOUT_SEC, ChronoUnit.SECONDS))
            .GET()
            .build();
    }

    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    private HttpClient buildCruiseControlApiClient() throws Exception {
        HttpClient.Builder builder = HttpClient.newBuilder().executor(httpClientExecutor);
        if (config.cruiseControlSslEnabled()) {
            byte[] trustStoreContent = Files.readAllBytes(new File(TRUSTSTORE_FILE).toPath());
            String trustStorePassword = new String(Files.readAllBytes(new File(TRUSTSTORE_PASS).toPath()), US_ASCII);
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(new ByteArrayInputStream(trustStoreContent), trustStorePassword.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
            tmf.init(ks);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
            builder.sslContext(sslContext);
        }
        if (config.cruiseControlAuthEnabled()) {
            String username = new String(Files.readAllBytes(new File(API_USERNAME).toPath()), US_ASCII);
            char[] password = new String(Files.readAllBytes(new File(API_PASSWORD).toPath()), US_ASCII).toCharArray();
            builder.authenticator(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
            });
        }
        return builder.build();
    }

    private void pending(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                    .withState("pending").withTargetReplicas(reconcilableTopic.kt().getSpec().getReplicas()).build()).build()));
    }
    
    private void ongoing(List<ReconcilableTopic> reconcilableTopics, String message, String userTaskId) {
        LOGGER.infoOp("{}, topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withState("ongoing").withSessionId(userTaskId).endReplicasChange().build()));
    }
    
    private void completed(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(null).build()));
    }
    
    private void failed(List<ReconcilableTopic> reconcilableTopics, String message, HttpResponse<String> response) {
        String text = format("%s, %s", message, parseError(response));
        LOGGER.errorOp("{}, topics: {}", text, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withMessage(text).endReplicasChange().build()));
    }

    private String parseError(HttpResponse<String> response) {
        if (response != null && response.body() != null) {
            try {
                ErrorResponse errorResponse = mapper.readValue(response.body(), ErrorResponse.class);
                if (errorResponse.errorMessage().contains("NotEnoughValidWindowsException")) {
                    return "Cluster model not ready";
                } else if (errorResponse.errorMessage().contains("OngoingExecutionException")
                    || errorResponse.errorMessage().contains("stop_ongoing_execution")) {
                    return "Another task is executing";
                } else {
                    return errorResponse.errorMessage();
                }
            } catch (Throwable e) {
                LOGGER.errorOp("Failed to parser error response {}: {}", response, e.getMessage());
            }
        }
        return "Empty response";
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

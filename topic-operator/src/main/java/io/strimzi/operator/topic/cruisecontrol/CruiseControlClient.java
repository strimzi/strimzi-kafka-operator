/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.api.kafka.model.topic.KafkaTopic;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Cruise Control client.
 * <br/><br/>
 * The server runs one task execution at a time, additional 
 * requests are queued up to {@code max.active.user.tasks}.
 */
public interface CruiseControlClient {
    /**
     * HTTP request timeout in seconds.
     */
    long HTTP_REQUEST_TIMEOUT_SEC = 60;

    /**
     * Create a new Cruise Control client with the given configuration.
     *
     * @param config Configuration.
     * @return Cruise Control client.
     */
    static CruiseControlClient create(Config config) {
        return new CruiseControlClientImpl(config);
    }

    /**
     * Close the client.
     */
    void close();

    /**
     * Send a POST request to {@code topic_configuration} endpoint.
     * This can be used to request replication factor changes (async operation).
     * 
     * @param kafkaTopics List of Kafka topics.
     * @return The user task id.
     */
    String topicConfiguration(List<KafkaTopic> kafkaTopics);

    /**
     * Send a GET request to {@code user_tasks} endpoint.
     * This can be used to check a task execution result (sync operation).
     * 
     * @param userTaskIds Set of user task ids.
     * @return User tasks response.
     */
    UserTasksResponse userTasks(Set<String> userTaskIds);

    /**
     * Get the error message from HTTP response.
     * 
     * @param response The HTTP response.
     * @return The error message.
     */
    Optional<String> errorMessage(HttpResponse<String> response);

    /**
     * Client configuration.
     */
    class Config {
        private final String serverHostname;
        private final int serverPort;
        private final boolean rackEnabled;
        private final boolean sslEnabled;
        private final byte[] sslCertificate;
        private final boolean authEnabled;
        private final String authUsername;
        private final String authPassword;

        /**
         * Create new configuration.
         *
         * @param serverHostname Server hostname.
         * @param serverPort Server port.
         * @param rackEnabled Whether rack awareness is enabled.
         * @param sslEnabled Whether SSL is enabled.
         * @param sslCertificate SSL certificate.
         * @param authEnabled Whether authentication is enabled.
         * @param authUsername Authentication username.
         * @param authPassword Authentication password.
         */
        public Config(String serverHostname,
                      int serverPort,
                      boolean rackEnabled,
                      boolean sslEnabled,
                      byte[] sslCertificate,
                      boolean authEnabled,
                      String authUsername,
                      String authPassword) {
            if (serverHostname == null || serverHostname.isBlank()) {
                throw new IllegalArgumentException("Hostname is not set");
            }
            if (serverPort <= 0) {
                throw new IllegalArgumentException("Port number is invalid");
            }
            if (sslEnabled && (sslCertificate == null || sslCertificate.length == 0)) {
                throw new IllegalArgumentException("SSL certificate is not set");
            }
            if (authEnabled && (authUsername == null || authUsername.isBlank())) {
                throw new IllegalArgumentException("Authentication username is not set");
            }
            if (authEnabled && (authPassword == null || authPassword.isBlank())) {
                throw new IllegalArgumentException("Authentication password is not set");
            }

            this.serverHostname = serverHostname;
            this.serverPort = serverPort;
            this.rackEnabled = rackEnabled;
            this.sslEnabled = sslEnabled;
            this.sslCertificate = sslCertificate;
            this.authEnabled = authEnabled;
            this.authUsername = authUsername;
            this.authPassword = authPassword;
        }

        /** @return Server hostname. */
        public String serverHostname() {
            return serverHostname;
        }

        /** @return Server port. */
        public int serverPort() {
            return serverPort;
        }

        /** @return Rack enabled. */
        public boolean rackEnabled() {
            return rackEnabled;
        }

        /** @return SSL enabled. */
        public boolean sslEnabled() {
            return sslEnabled;
        }

        /** @return SSL certificate. */
        public byte[] sslCertificate() {
            return sslCertificate;
        }

        /** @return Authentication enabled. */
        public boolean authEnabled() {
            return authEnabled;
        }

        /** @return Authentication username. */
        public String authUsername() {
            return authUsername;
        }

        /** @return Authentication password. */
        public String authPassword() {
            return authPassword;
        }
    }
    
    /**
     * Topic names grouped by replication factor value.
     * In order to support batch requests, we send a JSON payload where, for each RF value, 
     * we have a simple regex like topic1|topic2|topic3 (i.e. a group by operation).
     * 
     * @param topicByReplicationFactor Topic names grouped by replication factor value.
     */
    record ReplicationFactor(@JsonProperty("topic_by_replication_factor") Map<Integer, String> topicByReplicationFactor) { }

    /**
     * Replication factor changes.
     * 
     * @param replicationFactor Replication factor value.
     */
    record ReplicationFactorChanges(@JsonProperty("replication_factor") ReplicationFactor replicationFactor) { }
    
    /**
     * The user task.
     * 
     * @param status Status.
     * @param clientIdentity Client identity.
     * @param requestURL Request URL.
     * @param userTaskId User task id.
     * @param startMs Start time in ms.
     */
    record UserTask(
        @JsonProperty("Status") String status,
        @JsonProperty("ClientIdentity") String clientIdentity,
        @JsonProperty("RequestURL") String requestURL,
        @JsonProperty("UserTaskId") String userTaskId,
        @JsonProperty("StartMs") long startMs
    ) { }

    /**
     * The user tasks response.
     * 
     * @param userTasks User task list.
     * @param version Version.
     */
    record UserTasksResponse(List<UserTask> userTasks, int version) { }

    /**
     * The error response.
     * 
     * @param stackTrace Stack trace.
     * @param errorMessage Error message.
     * @param version Version.
     */
    record ErrorResponse(String stackTrace, String errorMessage, int version) { }

    /**
     * Task states.
     */
    enum TaskState {
        /**
         * The task has been accepted and waiting for execution.
         */
        ACTIVE("Active"),

        /**
         * The task is being executed.
         */
        IN_EXECUTION("InExecution"),

        /**
         * The task has been completed.
         */
        COMPLETED("Completed"),

        /**
         * The task has been completed with errors.
         */
        COMPLETED_WITH_ERROR("CompletedWithError");
        
        private final String value;
        TaskState(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

        /**
         * Get the enum constant by value.
         * 
         * @param value Value.
         * @return Constant.
         */
        public static TaskState get(String value) {
            Optional<TaskState> constant = List.of(values()).stream()
                .filter(v -> v.toString().equals(value)).findFirst();
            return constant.orElseThrow();
        }
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import java.util.HashMap;
import java.util.Map;


/**
 * User Operator Configuration Builder class
 */
public class UserOperatorConfigBuilder {

    private final Map<String, String> map;

    protected  UserOperatorConfigBuilder() {
        this.map = new HashMap<>();
    }

    protected  UserOperatorConfigBuilder(UserOperatorConfig config) {
        this.map = new HashMap<>();
        map.put(UserOperatorConfig.STRIMZI_NAMESPACE, config.getNamespace());
        map.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, String.valueOf(config.getReconciliationIntervalMs()));
        map.put(UserOperatorConfig.STRIMZI_KAFKA_BOOTSTRAP_SERVERS, config.getKafkaBootstrapServers());
        map.put(UserOperatorConfig.STRIMZI_LABELS, config.getLabels().toSelectorString());
        map.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, config.getCaCertSecretName());
        map.put(UserOperatorConfig.STRIMZI_CLUSTER_CA_CERT_SECRET_NAME, config.getClusterCaCertSecretName());
        map.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, config.getCaKeySecretName());
        map.put(UserOperatorConfig.STRIMZI_EO_KEY_SECRET_NAME, config.getEuoKeySecretName());
        map.put(UserOperatorConfig.STRIMZI_CA_NAMESPACE, config.getCaNamespace());
        map.put(UserOperatorConfig.STRIMZI_SECRET_PREFIX, config.getSecretPrefix());
        map.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL, String.valueOf(config.getClientsCaRenewalDays()));
        map.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY, String.valueOf(config.getClientsCaValidityDays()));
        map.put(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED, String.valueOf(config.isAclsAdminApiSupported()));
        map.put(UserOperatorConfig.STRIMZI_KRAFT_ENABLED, String.valueOf(config.isKraftEnabled()));
        map.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, String.valueOf(config.getScramPasswordLength()));
        map.put(UserOperatorConfig.STRIMZI_MAINTENANCE_TIME_WINDOWS, config.get(UserOperatorConfig.MAINTENANCE_TIME_WINDOWS));
        map.put(UserOperatorConfig.STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION, config.get(UserOperatorConfig.KAFKA_ADMIN_CLIENT_CONFIGURATION));
        map.put(UserOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, String.valueOf(config.getOperationTimeoutMs()));
        map.put(UserOperatorConfig.STRIMZI_WORK_QUEUE_SIZE, String.valueOf(config.getWorkQueueSize()));
        map.put(UserOperatorConfig.STRIMZI_CONTROLLER_THREAD_POOL_SIZE, String.valueOf(config.getControllerThreadPoolSize()));
        map.put(UserOperatorConfig.STRIMZI_CACHE_REFRESH_INTERVAL_MS, String.valueOf(config.getCacheRefresh()));
        map.put(UserOperatorConfig.STRIMZI_BATCH_QUEUE_SIZE, String.valueOf(config.getBatchQueueSize()));
        map.put(UserOperatorConfig.STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE, String.valueOf(config.getBatchMaxBlockSize()));
        map.put(UserOperatorConfig.STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS, String.valueOf(config.getBatchMaxBlockTime()));
        map.put(UserOperatorConfig.STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE, String.valueOf(config.getUserOperationsThreadPoolSize()));
    }


    protected UserOperatorConfigBuilder withNamespace(String namespace) {
        map.put(UserOperatorConfig.STRIMZI_NAMESPACE, namespace);
        return this;
    }

    protected UserOperatorConfigBuilder withReconciliationIntervalMs(long reconciliationIntervalMs) {
        map.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, String.valueOf(reconciliationIntervalMs));
        return this;
    }

    protected UserOperatorConfigBuilder withKafkaBootstrapServers(String kafkaBootstrapServers) {
        map.put(UserOperatorConfig.STRIMZI_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        return this;

    }

    protected UserOperatorConfigBuilder withLabels(String labels) {
        map.put(UserOperatorConfig.STRIMZI_LABELS, labels);
        return this;

    }

    protected UserOperatorConfigBuilder withCaCertSecretName(String caCertSecretName) {
        map.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, caCertSecretName);
        return this;

    }

    protected UserOperatorConfigBuilder withCaKeySecretName(String caKeySecretName) {
        map.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, caKeySecretName);
        return this;

    }

    protected UserOperatorConfigBuilder withClusterCaCertSecretName(String clusterCaCertSecretName) {
        map.put(clusterCaCertSecretName, clusterCaCertSecretName);
        return this;

    }

    protected UserOperatorConfigBuilder withEuoKeySecretName(String euoKeySecretName) {
        map.put(UserOperatorConfig.STRIMZI_EO_KEY_SECRET_NAME, euoKeySecretName);
        return this;

    }

    protected UserOperatorConfigBuilder withCaNamespace(String caNamespace) {
        map.put(UserOperatorConfig.STRIMZI_CA_NAMESPACE, caNamespace);
        return this;

    }

    protected UserOperatorConfigBuilder withSecretPrefix(String secretPrefix) {
        map.put(UserOperatorConfig.STRIMZI_SECRET_PREFIX, secretPrefix);
        return this;

    }

    protected UserOperatorConfigBuilder withClientsCaValidityDays(int clientsCaValidityDays) {
        map.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY, String.valueOf(clientsCaValidityDays));
        return this;

    }

    protected UserOperatorConfigBuilder withClientsCaRenewalDays(int clientsCaRenewalDays) {
        map.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL, String.valueOf(clientsCaRenewalDays));
        return this;

    }

    protected UserOperatorConfigBuilder withAclsAdminApiSupported(boolean aclsAdminApiSupported) {
        map.put(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED, String.valueOf(aclsAdminApiSupported));
        return this;

    }

    protected UserOperatorConfigBuilder withKraftEnabled(boolean kraftEnabled) {
        map.put(UserOperatorConfig.STRIMZI_KRAFT_ENABLED, String.valueOf(kraftEnabled));
        return this;

    }

    protected UserOperatorConfigBuilder withScramPasswordLength(int scramPasswordLength) {
        map.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, String.valueOf(scramPasswordLength));
        return this;

    }

    protected UserOperatorConfigBuilder withMaintenanceWindows(String maintenanceWindows) {
        map.put(UserOperatorConfig.STRIMZI_MAINTENANCE_TIME_WINDOWS, maintenanceWindows);
        return this;

    }

    protected UserOperatorConfigBuilder withKafkaAdminClientConfiguration(String kafkaAdminClientConfiguration) {
        map.put(UserOperatorConfig.STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION, kafkaAdminClientConfiguration);
        return this;

    }

    protected UserOperatorConfigBuilder withOperationTimeoutMs(long operationTimeoutMs) {
        map.put(UserOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, String.valueOf(operationTimeoutMs));
        return this;

    }

    protected UserOperatorConfigBuilder withWorkQueueSize(int workQueueSize) {
        map.put(UserOperatorConfig.STRIMZI_WORK_QUEUE_SIZE, String.valueOf(workQueueSize));
        return this;

    }

    protected UserOperatorConfigBuilder withControllerThreadPoolSize(int controllerThreadPoolSize) {
        map.put(UserOperatorConfig.STRIMZI_CONTROLLER_THREAD_POOL_SIZE, String.valueOf(controllerThreadPoolSize));
        return this;

    }

    protected UserOperatorConfigBuilder withCacheRefresh(long cacheRefresh) {
        map.put(UserOperatorConfig.STRIMZI_CACHE_REFRESH_INTERVAL_MS, String.valueOf(cacheRefresh));
        return this;

    }

    protected UserOperatorConfigBuilder withBatchQueueSize(int batchQueueSize) {
        map.put(UserOperatorConfig.STRIMZI_BATCH_QUEUE_SIZE, String.valueOf(batchQueueSize));
        return this;

    }

    protected UserOperatorConfigBuilder withBatchMaxBlockSize(int batchMaxBlockSize) {
        map.put(UserOperatorConfig.STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE, String.valueOf(batchMaxBlockSize));
        return this;

    }

    protected UserOperatorConfigBuilder withBatchMaxBlockTime(int batchMaxBlockTime) {
        map.put(UserOperatorConfig.STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS, String.valueOf(batchMaxBlockTime));
        return this;

    }

    protected UserOperatorConfigBuilder withUserOperationsThreadPoolSize(int userOperationsThreadPoolSize) {
        map.put(UserOperatorConfig.STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE, String.valueOf(userOperationsThreadPoolSize));
        return this;
    }

    protected UserOperatorConfig build() {
        return new UserOperatorConfig(this.map);
    }
}

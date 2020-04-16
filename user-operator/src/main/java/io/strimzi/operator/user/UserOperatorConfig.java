/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;

/**
 * Cluster Operator configuration
 */
public class UserOperatorConfig {

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String STRIMZI_LABELS = "STRIMZI_LABELS";
    public static final String STRIMZI_CA_CERT_SECRET_NAME = "STRIMZI_CA_CERT_NAME";
    public static final String STRIMZI_CA_KEY_SECRET_NAME = "STRIMZI_CA_KEY_NAME";
    public static final String STRIMZI_CLUSTER_CA_CERT_SECRET_NAME = "STRIMZI_CLUSTER_CA_CERT_SECRET_NAME";
    public static final String STRIMZI_EO_KEY_SECRET_NAME = "STRIMZI_EO_KEY_SECRET_NAME";
    public static final String STRIMZI_CA_NAMESPACE = "STRIMZI_CA_NAMESPACE";
    public static final String STRIMZI_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String STRIMZI_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String STRIMZI_CLIENTS_CA_VALIDITY = "STRIMZI_CA_VALIDITY";
    public static final String STRIMZI_CLIENTS_CA_RENEWAL = "STRIMZI_CA_RENEWAL";

    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    public static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9091";
    public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final long DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = 6_000;

    private final String namespace;
    private final long reconciliationIntervalMs;
    private final String kafkaBootstrapServers;
    private final String zookeperConnect;
    private final long zookeeperSessionTimeoutMs;
    private Labels labels;
    private final String caCertSecretName;
    private final String caKeySecretName;
    private final String clusterCaCertSecretName;
    private final String eoKeySecretName;
    private final String caNamespace;

    /**
     * Constructor
     *
     * @param namespace namespace in which the operator will run and create resources.
     * @param reconciliationIntervalMs How many milliseconds between reconciliation runs.
     * @param kafkaBootstrapServers Kafka bootstrap servers list
     * @param zookeperConnect Connecton URL for Zookeeper.
     * @param zookeeperSessionTimeoutMs Session timeout for Zookeeper connections.
     * @param labels Map with labels which should be used to find the KafkaUser resources.
     * @param caCertSecretName Name of the secret containing the clients Certification Authority certificate.
     * @param caKeySecretName The name of the secret containing the clients Certification Authority key.
     * @param clusterCaCertSecretName Name of the secret containing the cluster Certification Authority certificate.
     * @param eoKeySecretName The name of the secret containing the Entity Operator key and certificate
     * @param caNamespace Namespace with the CA secret.
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"}) //TODO: to remove when removing the zookeeper related parameters
    public UserOperatorConfig(String namespace,
                              long reconciliationIntervalMs,
                              String kafkaBootstrapServers,
                              String zookeperConnect,
                              long zookeeperSessionTimeoutMs,
                              Labels labels, String caCertSecretName,
                              String caKeySecretName,
                              String clusterCaCertSecretName,
                              String eoKeySecretName,
                              String caNamespace) {
        this.namespace = namespace;
        this.reconciliationIntervalMs = reconciliationIntervalMs;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.zookeperConnect = zookeperConnect;
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
        this.labels = labels;
        this.caCertSecretName = caCertSecretName;
        this.caKeySecretName = caKeySecretName;
        this.clusterCaCertSecretName = clusterCaCertSecretName;
        this.eoKeySecretName = eoKeySecretName;
        this.caNamespace = caNamespace;
    }

    /**
     * Loads configuration parameters from a related map
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Operator configuration instance
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static UserOperatorConfig fromMap(Map<String, String> map) {

        String namespace = map.get(UserOperatorConfig.STRIMZI_NAMESPACE);
        if (namespace == null || namespace.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_NAMESPACE + " cannot be null");
        }


        long reconciliationInterval = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
        String reconciliationIntervalEnvVar = map.get(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
        if (reconciliationIntervalEnvVar != null) {
            reconciliationInterval = Long.parseLong(reconciliationIntervalEnvVar);
        }

        String kafkaBootstrapServers = DEFAULT_KAFKA_BOOTSTRAP_SERVERS;
        String kafkaBootstrapServersEnvVar = map.get(UserOperatorConfig.STRIMZI_KAFKA_BOOTSTRAP_SERVERS);
        if (kafkaBootstrapServersEnvVar != null && !kafkaBootstrapServersEnvVar.isEmpty()) {
            kafkaBootstrapServers = kafkaBootstrapServersEnvVar;
        }

        String zookeeperConnect = DEFAULT_ZOOKEEPER_CONNECT;
        String zookeeperConnectEnvVar = map.get(UserOperatorConfig.STRIMZI_ZOOKEEPER_CONNECT);
        if (zookeeperConnectEnvVar != null && !zookeeperConnectEnvVar.isEmpty()) {
            zookeeperConnect = zookeeperConnectEnvVar;
        }

        long zookeeperSessionTimeoutMs = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
        String zookeeperSessionTimeoutMsEnvVar = map.get(UserOperatorConfig.STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS);
        if (zookeeperSessionTimeoutMsEnvVar != null) {
            zookeeperSessionTimeoutMs = Long.parseLong(zookeeperSessionTimeoutMsEnvVar);
        }

        Labels labels;
        try {
            labels = Labels.fromString(map.get(STRIMZI_LABELS));
        } catch (Exception e)   {
            throw new InvalidConfigurationException("Failed to parse labels from " + STRIMZI_LABELS, e);
        }

        String caCertSecretName = map.get(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME);
        if (caCertSecretName == null || caCertSecretName.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME + " cannot be null");
        }

        String caKeySecretName = map.get(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME);
        if (caKeySecretName == null || caKeySecretName.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME + " cannot be null");
        }

        String clusterCaCertSecretName = map.get(UserOperatorConfig.STRIMZI_CLUSTER_CA_CERT_SECRET_NAME);

        String eoKeySecretName = map.get(UserOperatorConfig.STRIMZI_EO_KEY_SECRET_NAME);

        String caNamespace = map.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE);
        if (caNamespace == null || caNamespace.isEmpty()) {
            caNamespace = namespace;
        }

        return new UserOperatorConfig(namespace, reconciliationInterval, kafkaBootstrapServers, zookeeperConnect, zookeeperSessionTimeoutMs, labels,
                caCertSecretName, caKeySecretName, clusterCaCertSecretName, eoKeySecretName, caNamespace);
    }

    public static int getClientsCaValidityDays() {
        return getIntProperty(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY, CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS);
    }

    public static int getClientsCaRenewalDays() {
        return getIntProperty(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL, CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS);
    }

    private static int getIntProperty(String name, int defaultVal) {
        String env = System.getenv(name);
        if (env != null) {
            return Integer.parseInt(env);
        } else {
            return defaultVal;
        }
    }

    /**
     * @return  namespace in which the operator runs and creates resources
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    /**
     * @return  The labels which should be used as selecter
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * @return  The name of the secret with the Client CA
     */
    public String getCaCertSecretName() {
        return caCertSecretName;
    }

    /**
     * @return  The name of the secret with the Client CA
     */
    public String getCaKeySecretName() {
        return caKeySecretName;
    }

    /**
     * @return  The name of the secret with the Cluster CA
     */
    public String getClusterCaCertSecretName() {
        return clusterCaCertSecretName;
    }

    /**
     * @return  The name of the secret with Entity Operator key and certificate
     */
    public String getEoKeySecretName() {
        return eoKeySecretName;
    }

    /**
     * @return  The namespace of the Client CA
     */
    public String getCaNamespace() {
        return caNamespace;
    }

    /**
     * @return  Kafka bootstrap servers list
     */
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    /**
     * @return  Zookeeper connection URL
     */
    public String getZookeperConnect() {
        return zookeperConnect;
    }

    /**
     * @return  Zookeeepr connection and session timeout
     */
    public long getZookeeperSessionTimeoutMs() {
        return zookeeperSessionTimeoutMs;
    }

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "namespace=" + namespace +
                ",reconciliationIntervalMs=" + reconciliationIntervalMs +
                ",kafkaBootstrapServers=" + kafkaBootstrapServers +
                ",zookeperConnect=" + zookeperConnect +
                ",zookeeperSessionTimeoutMs=" + zookeeperSessionTimeoutMs +
                ",labels=" + labels +
                ",caName=" + caCertSecretName +
                ",clusterCaCertSecretName=" + clusterCaCertSecretName +
                ",eoKeySecretName=" + eoKeySecretName +
                ",caNamespace=" + caNamespace +
                ")";
    }
}

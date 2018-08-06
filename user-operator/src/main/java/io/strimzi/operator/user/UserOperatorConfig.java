/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

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
    public static final String STRIMZI_CA_NAME = "STRIMZI_CA_NAME";
    public static final String STRIMZI_CA_NAMESPACE = "STRIMZI_CA_NAMESPACE";
    public static final String STRIMZI_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";

    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final long DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = 6_000;

    private final String namespace;
    private final long reconciliationIntervalMs;
    private final String zookeperConnect;
    private final long zookeeperSessionTimeoutMs;
    private Labels labels;
    private final String caName;
    private final String caNamespace;

    /**
     * Constructor
     *
     * @param namespace namespace in which the operator will run and create resources
     * @param reconciliationIntervalMs    specify every how many milliseconds the reconciliation runs
     * @param zookeperConnect Connecton URL for Zookeeper
     * @param zookeeperSessionTimeoutMs Session timeout for Zookeeper connections
     * @param labels    Map with labels which should be used to find the KAfkaUser resources
     * @param caName    Name of the secret containing the Certification Authority
     * @param caNamespace   Namespace with the CA secret
     */
    public UserOperatorConfig(String namespace, long reconciliationIntervalMs, String zookeperConnect, long zookeeperSessionTimeoutMs, Labels labels, String caName, String caNamespace) {
        this.namespace = namespace;
        this.reconciliationIntervalMs = reconciliationIntervalMs;
        this.zookeperConnect = zookeperConnect;
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
        this.labels = labels;
        this.caName = caName;
        this.caNamespace = caNamespace;
    }

    /**
     * Loads configuration parameters from a related map
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Operator configuration instance
     */
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

        String caName = map.get(UserOperatorConfig.STRIMZI_CA_NAME);
        if (caName == null || caName.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_CA_NAME + " cannot be null");
        }

        String caNamespace = map.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE);
        if (caNamespace == null || caNamespace.isEmpty()) {
            caNamespace = namespace;
        }

        return new UserOperatorConfig(namespace, reconciliationInterval, zookeeperConnect, zookeeperSessionTimeoutMs, labels, caName, caNamespace);
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
    public String getCaName() {
        return caName;
    }

    /**
     * @return  The namespace of the Client CA
     */
    public String getCaNamespace() {
        return caNamespace;
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
                ",zookeperConnect=" + zookeperConnect +
                ",zookeeperSessionTimeoutMs=" + zookeeperSessionTimeoutMs +
                ",labels=" + labels +
                ",caName=" + caName +
                ",caNamespace=" + caNamespace +
                ")";
    }
}

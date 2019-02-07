/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.operator.resource.AbstractWatchableResourceOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * Cluster Operator configuration
 */
public class ClusterOperatorConfig {

    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorConfig.class);

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String STRIMZI_OPERATION_TIMEOUT_MS = "STRIMZI_OPERATION_TIMEOUT_MS";
    public static final String STRIMZI_CREATE_CLUSTER_ROLES = "STRIMZI_CREATE_CLUSTER_ROLES";
    public static final String STRIMZI_KAFKA_IMAGES = "STRIMZI_KAFKA_IMAGES";
    public static final String STRIMZI_KAFKA_CONNECT_IMAGES = "STRIMZI_KAFKA_CONNECT_IMAGES";
    public static final String STRIMZI_KAFKA_CONNECT_S2I_IMAGES = "STRIMZI_KAFKA_CONNECT_S2I_IMAGES";
    public static final String STRIMZI_KAFKA_MIRROR_MAKER_IMAGES = "STRIMZI_KAFKA_MIRROR_MAKER_IMAGES";
    public static final String STRIMZI_IMAGE_PULL_POLICY = "STRIMZI_IMAGE_PULL_POLICY";

    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    public static final long DEFAULT_OPERATION_TIMEOUT_MS = 300_000;
    public static final boolean DEFAULT_CREATE_CLUSTER_ROLES = false;

    private final Set<String> namespaces;
    private final long reconciliationIntervalMs;
    private final long operationTimeoutMs;
    private final boolean createClusterRoles;
    private final KafkaVersion.Lookup versions;
    private final String imagePullPolicy;

    /**
     * Constructor
     *
     * @param namespaces namespace in which the operator will run and create resources
     * @param reconciliationIntervalMs    specify every how many milliseconds the reconciliation runs
     * @param operationTimeoutMs    timeout for internal operations specified in milliseconds
     * @param createClusterRoles true to create the cluster roles
     * @param versions The configured Kafka versions
     */
    public ClusterOperatorConfig(Set<String> namespaces, long reconciliationIntervalMs, long operationTimeoutMs, boolean createClusterRoles, KafkaVersion.Lookup versions, String imagePullPolicy) {
        this.namespaces = unmodifiableSet(new HashSet<>(namespaces));
        this.reconciliationIntervalMs = reconciliationIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
        this.createClusterRoles = createClusterRoles;
        this.versions = versions;
        this.imagePullPolicy = imagePullPolicy;
    }

    /**
     * Loads configuration parameters from a related map
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Operator configuration instance
     */
    public static ClusterOperatorConfig fromMap(Map<String, String> map) {

        String namespacesList = map.get(ClusterOperatorConfig.STRIMZI_NAMESPACE);
        Set<String> namespaces;
        if (namespacesList == null || namespacesList.isEmpty()) {
            throw new InvalidConfigurationException(ClusterOperatorConfig.STRIMZI_NAMESPACE + " cannot be null");
        } else {
            if (namespacesList.trim().equals(AbstractWatchableResourceOperator.ANY_NAMESPACE)) {
                namespaces = Collections.singleton(AbstractWatchableResourceOperator.ANY_NAMESPACE);
            } else if (namespacesList.matches("(\\s?[a-z0-9.-]+\\s?,)*\\s?[a-z0-9.-]+\\s?")) {
                namespaces = new HashSet(asList(namespacesList.trim().split("\\s*,+\\s*")));
            } else {
                throw new InvalidConfigurationException(ClusterOperatorConfig.STRIMZI_NAMESPACE
                        + " is not a valid list of namespaces nor the 'any namespace' wildcard "
                        + AbstractWatchableResourceOperator.ANY_NAMESPACE);
            }
        }

        long reconciliationInterval = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
        String reconciliationIntervalEnvVar = map.get(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
        if (reconciliationIntervalEnvVar != null) {
            reconciliationInterval = Long.parseLong(reconciliationIntervalEnvVar);
        }

        long operationTimeout = DEFAULT_OPERATION_TIMEOUT_MS;
        String operationTimeoutEnvVar = map.get(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS);
        if (operationTimeoutEnvVar != null) {
            operationTimeout = Long.parseLong(operationTimeoutEnvVar);
        }

        boolean createClusterRoles = DEFAULT_CREATE_CLUSTER_ROLES;
        String createClusterRolesEnvVar = map.get(ClusterOperatorConfig.STRIMZI_CREATE_CLUSTER_ROLES);
        if (createClusterRolesEnvVar != null) {
            createClusterRoles = Boolean.parseBoolean(createClusterRolesEnvVar);
        }

        String imagePullPolicy = null;
        String imagePullPolicyEnvVar = map.get(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY);
        if (imagePullPolicyEnvVar != null) {
            switch (imagePullPolicyEnvVar.toLowerCase(Locale.ENGLISH))  {
                case "always":
                    imagePullPolicy = "Always";
                    break;
                case "ifnotpresent":
                    imagePullPolicy = "IfNotPresent";
                    break;
                case "never":
                    imagePullPolicy = "Never";
                    break;
                default:
                    throw new InvalidConfigurationException(imagePullPolicyEnvVar
                            + " is not a valid " + ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY + " value. " +
                            ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY + " can have one of the following values: Always, IfNotPresent, Never.");
            }

            createClusterRoles = Boolean.parseBoolean(createClusterRolesEnvVar);
        }

        KafkaVersion.Lookup lookup = new KafkaVersion.Lookup(
                ModelUtils.parseImageMap(map.get(STRIMZI_KAFKA_IMAGES)),
                ModelUtils.parseImageMap(map.get(STRIMZI_KAFKA_CONNECT_IMAGES)),
                ModelUtils.parseImageMap(map.get(STRIMZI_KAFKA_CONNECT_S2I_IMAGES)),
                ModelUtils.parseImageMap(map.get(STRIMZI_KAFKA_MIRROR_MAKER_IMAGES)));
        for (String version : lookup.supportedVersions()) {
            if (lookup.kafkaImage(null, version) == null) {
                LOGGER.warn("{} does not provide an image for version {}", STRIMZI_KAFKA_IMAGES, version);
            }
            if (lookup.kafkaConnectVersion(null, version) == null) {
                LOGGER.warn("{} does not provide an image for version {}", STRIMZI_KAFKA_CONNECT_IMAGES, version);
            }
            // Need to know whether we're on OS to decide whether to valid s2i
            if (lookup.kafkaMirrorMakerImage(null, version) == null) {
                LOGGER.warn("{} does not provide an image for version {}", STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, version);
            }
        }

        return new ClusterOperatorConfig(namespaces, reconciliationInterval, operationTimeout, createClusterRoles, lookup, imagePullPolicy);
    }


    /**
     * @return  namespaces in which the operator runs and creates resources
     */
    public Set<String> getNamespaces() {
        return namespaces;
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    /**
     * @return  how many milliseconds should we wait for Kubernetes operations
     */
    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    /**
     * @return  Indicates whether Cluster Roles should be created
     */
    public boolean isCreateClusterRoles() {
        return createClusterRoles;
    }

    public KafkaVersion.Lookup versions() {
        return versions;
    }

    public String getImagePullPolicy() {
        return imagePullPolicy;
    }

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "namespaces=" + namespaces +
                ",reconciliationIntervalMs=" + reconciliationIntervalMs +
                ",operationTimeoutMs=" + operationTimeoutMs +
                ",createClusterRoles=" + createClusterRoles +
                ",versions=" + versions +
                ",imagePullPolicy=" + imagePullPolicy +
                ")";
    }
}

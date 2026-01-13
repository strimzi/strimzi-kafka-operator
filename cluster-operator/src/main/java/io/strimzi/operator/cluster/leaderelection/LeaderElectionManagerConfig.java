/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.strimzi.operator.common.config.ConfigParameter;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.config.ConfigParameterParser.DURATION;
import static io.strimzi.operator.common.config.ConfigParameterParser.NON_EMPTY_STRING;

/**
 * Configuration class for the Leader Election Manager
 */
public class LeaderElectionManagerConfig {

    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();

    /**
     * Name of the Kubernetes Lease resource
     */
    public final static ConfigParameter<String> ENV_VAR_LEADER_ELECTION_LEASE_NAME = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_LEASE_NAME", NON_EMPTY_STRING, null, CONFIG_VALUES);

    /**
     * Namespace of the Kubernetes Lease resource
     */
    public final static ConfigParameter<String>  ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE", NON_EMPTY_STRING, null, CONFIG_VALUES);

    /**
     * Identity of this operator for claiming the leadership
     */
    public final static ConfigParameter<String>  ENV_VAR_LEADER_ELECTION_IDENTITY = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_IDENTITY", NON_EMPTY_STRING, null, CONFIG_VALUES);

    /**
     * Duration of the leadership
     */
    public final static ConfigParameter<Duration> ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_LEASE_DURATION_MS", DURATION, "15000", CONFIG_VALUES);

    /**
     * Hw often should the leadership be renewed
     */
    public final static ConfigParameter<Duration> ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_RENEW_DEADLINE_MS", DURATION, "10000", CONFIG_VALUES);

    /**
     * Retry period for accessing the Lease resource
     */
    public final static ConfigParameter<Duration> ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_RETRY_PERIOD_MS", DURATION, "2000", CONFIG_VALUES);

    /**
     * Creates the LeaderElectionManager configuration from Map with environment variables
     *
     * @param map   Map with environment variables
     *
     * @return  Instance of LeaderElectionManagerConfig
     */

    public static LeaderElectionManagerConfig fromMap(Map<String, String> map) {

        Map<String, String> envMap = new HashMap<>(map);

        envMap.keySet().retainAll(LeaderElectionManagerConfig.keyNames());

        Map<String, Object> generatedMap = ConfigParameter.define(envMap, CONFIG_VALUES);

        return new LeaderElectionManagerConfig(generatedMap);

    }

    /**
     * Creates the LeaderElectionManager configuration from existing map
     *
     * @param map   Map with environment variables
     *
     * @return  Instance of LeaderElectionManagerConfig
     */
    public static LeaderElectionManagerConfig buildFromExistingMap(Map<String, Object> map) {

        Map<String, Object> existingMap = new HashMap<>(map);

        existingMap.keySet().retainAll(LeaderElectionManagerConfig.keyNames());

        return new LeaderElectionManagerConfig(existingMap);

    }

    private final Map<String, Object> map;

    /**
     * Constructor
     *
     * @param map Map containing configurations and their respective values
     */

    private LeaderElectionManagerConfig(Map<String, Object> map) {
        this.map = map;
    }

    /**
     * Gets the set of configuration key names.
     *
     * @return Set of configuration key/names
     */
    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    /**
     * Gets the configuration values map.
     *
     * @return  Configuration values map
     */
    public static Map<String, ConfigParameter<?>> configValues() {
        return Collections.unmodifiableMap(CONFIG_VALUES);
    }

    @SuppressWarnings("unchecked")
    private  <T> T get(ConfigParameter<T> value) {
        return (T) this.map.get(value.key());
    }


    /**
     * Gets the name of the Kubernetes Lease resource.
     *
     * @return  Returns the name of the Kubernetes Lease resource
     */
    public String getLeaseName() {
        return get(ENV_VAR_LEADER_ELECTION_LEASE_NAME);
    }

    /**
     * Gets the namespace of the Kubernetes Lease resource.
     *
     * @return  Returns the namespace of the Kubernetes Lease resource
     */
    public String getNamespace() {
        return get(ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE);
    }

    /**
     * Gets the identity of this operator instance.
     *
     * @return  Returns the identity of this instance of the operator
     */
    public String getIdentity() {
        return get(ENV_VAR_LEADER_ELECTION_IDENTITY);
    }

    /**
     * Gets the duration for which the acquired lease is valid.
     *
     * @return  Returns the duration for which the acquired lease is valid
     */
    public Duration getLeaseDuration() {
        return get(ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS);
    }

    /**
     * Gets the duration for which the leader should retry to maintain leadership.
     *
     * @return  Returns the duration for which should the leader retry to maintain the leadership
     */
    public Duration getRenewDeadline() {
        return get(ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS);
    }

    /**
     * Gets how often the leader updates the lease lock.
     *
     * @return  Returns how often does the leader update the lease lock
     */
    public Duration getRetryPeriod() {
        return get(ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS);
    }

    @Override
    public String toString() {
        return "LeaderElectionConfig{" +
                "leaseName='" + getLeaseName() + '\'' +
                ", namespace='" + getNamespace() + '\'' +
                ", identity='" + getIdentity() + '\'' +
                ", leaseDuration=" + getLeaseDuration() +
                ", renewDeadline=" + getRenewDeadline() +
                ", retryPeriod=" + getRetryPeriod() +
                '}';
    }
}

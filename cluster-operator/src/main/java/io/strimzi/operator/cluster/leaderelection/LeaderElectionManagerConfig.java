/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.time.Duration;
import java.util.Map;

/**
 * Configuration class for the Leader Election Manager
 */
public class LeaderElectionManagerConfig {
    /**
     * Name of the Kubernetes Lease resource
     */
    public final static String ENV_VAR_LEADER_ELECTION_LEASE_NAME = "STRIMZI_LEADER_ELECTION_LEASE_NAME";

    /**
     * Namespace of the Kubernetes Lease resource
     */
    public final static String ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE = "STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE";

    /**
     * Identity of this operator for claiming the leadership
     */
    public final static String ENV_VAR_LEADER_ELECTION_IDENTITY = "STRIMZI_LEADER_ELECTION_IDENTITY";

    /**
     * Duration of the leadership
     */
    public final static String ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS = "STRIMZI_LEADER_ELECTION_LEASE_DURATION_MS";

    /**
     * Hw often should the leadership be renewed
     */
    public final static String ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS = "STRIMZI_LEADER_ELECTION_RENEW_DEADLINE_MS";

    /**
     * Retry period for accessing the Lease resource
     */
    public final static String ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS = "STRIMZI_LEADER_ELECTION_RETRY_PERIOD_MS";

    // Default values
    private final static Duration DEFAULT_STRIMZI_LEADER_ELECTION_LEASE_DURATION_MS = Duration.ofSeconds(15);
    private final static Duration DEFAULT_STRIMZI_LEADER_ELECTION_RENEW_DEADLINE_MS = Duration.ofSeconds(10);
    private final static Duration DEFAULT_STRIMZI_LEADER_ELECTION_RETRY_PERIOD_MS = Duration.ofSeconds(2);

    private final String leaseName;
    private final String namespace;
    private final String identity;
    private final Duration leaseDuration;
    private final Duration renewDeadline;
    private final Duration retryPeriod;

    /**
     * Constructs the LeaderElectionManagerConfig object
     *
     * @param leaseName     Name of the Kubernetes Lease resource
     * @param namespace     Namespace of the Kubernetes Lease resource
     * @param identity      Identity of this instance of the operator (should be unique: for example Pod name)
     * @param leaseDuration Duration for which the acquired lease is valid
     * @param renewDeadline Duration for which should the leader retry to maintain the leadership
     * @param retryPeriod   How often does the leader update the lease lock
     */
    public LeaderElectionManagerConfig(String leaseName, String namespace, String identity, Duration leaseDuration, Duration renewDeadline, Duration retryPeriod) {
        this.leaseName = leaseName;
        this.namespace = namespace;
        this.identity = identity;
        this.leaseDuration = leaseDuration;
        this.renewDeadline = renewDeadline;
        this.retryPeriod = retryPeriod;
    }

    /**
     * Creates the LeaderElectionManager configuration from Map with environment variables
     *
     * @param map   Map with environment variables
     *
     * @return  Instance of LeaderElectionManagerConfig
     */
    public static LeaderElectionManagerConfig fromMap(Map<String, String> map) {
        String leaseName = map.get(ENV_VAR_LEADER_ELECTION_LEASE_NAME);
        String namespace = map.get(ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE);
        String identity = map.get(ENV_VAR_LEADER_ELECTION_IDENTITY);

        if (leaseName == null || namespace == null || identity == null) {
            // Required options are not provided
            throw new InvalidConfigurationException("The " + ENV_VAR_LEADER_ELECTION_LEASE_NAME + ", " + ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE + " and " + ENV_VAR_LEADER_ELECTION_IDENTITY + " options are required and have to be set when leader election is enabled.");
        }

        Duration leaseDuration = parseDuration(map.get(ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS), DEFAULT_STRIMZI_LEADER_ELECTION_LEASE_DURATION_MS);
        Duration renewDeadline = parseDuration(map.get(ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS), DEFAULT_STRIMZI_LEADER_ELECTION_RENEW_DEADLINE_MS);
        Duration retryPeriod = parseDuration(map.get(ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS), DEFAULT_STRIMZI_LEADER_ELECTION_RETRY_PERIOD_MS);

        return new LeaderElectionManagerConfig(leaseName, namespace, identity, leaseDuration, renewDeadline, retryPeriod);
    }

    private static Duration parseDuration(String durationValue, Duration defaultDuration) {
        Duration duration = defaultDuration;

        if (durationValue != null) {
            duration = Duration.ofMillis(Long.parseLong(durationValue));
        }

        return duration;
    }

    /**
     * @return  Returns the name of the Kubernetes Lease resource
     */
    public String getLeaseName() {
        return leaseName;
    }

    /**
     * @return  Returns the namespace of the Kubernetes Lease resource
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @return  Returns the identity of this instance of the operator
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * @return  Returns the duration for which the acquired lease is valid
     */
    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    /**
     * @return  Returns the duration for which should the leader retry to maintain the leadership
     */
    public Duration getRenewDeadline() {
        return renewDeadline;
    }

    /**
     * @return  Returns how often does the leader update the lease lock
     */
    public Duration getRetryPeriod() {
        return retryPeriod;
    }

    @Override
    public String toString() {
        return "LeaderElectionConfig{" +
                "leaseName='" + leaseName + '\'' +
                ", namespace='" + namespace + '\'' +
                ", identity='" + identity + '\'' +
                ", leaseDuration=" + leaseDuration +
                ", renewDeadline=" + renewDeadline +
                ", retryPeriod=" + retryPeriod +
                '}';
    }
}

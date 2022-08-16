/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LeaderElectionManagerConfigTest {
    @Test
    public void testDefaultConfig() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME, "my-lease");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE, "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY, "my-pod");

        LeaderElectionManagerConfig config = LeaderElectionManagerConfig.fromMap(envVars);

        assertThat(config.getLeaseName(), is("my-lease"));
        assertThat(config.getNamespace(), is("my-namespace"));
        assertThat(config.getIdentity(), is("my-pod"));
        assertThat(config.getLeaseDuration().toMillis(), is(15_000L));
        assertThat(config.getRenewDeadline().toMillis(), is(10_000L));
        assertThat(config.getRetryPeriod().toMillis(), is(2_000L));
    }

    @Test
    public void testConfiguredTiming() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME, "my-lease");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE, "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY, "my-pod");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS, "30000");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS, "20000");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS, "5000");

        LeaderElectionManagerConfig config = LeaderElectionManagerConfig.fromMap(envVars);

        assertThat(config.getLeaseName(), is("my-lease"));
        assertThat(config.getNamespace(), is("my-namespace"));
        assertThat(config.getIdentity(), is("my-pod"));
        assertThat(config.getLeaseDuration().toMillis(), is(30_000L));
        assertThat(config.getRenewDeadline().toMillis(), is(20_000L));
        assertThat(config.getRetryPeriod().toMillis(), is(5_000L));
    }

    @Test
    public void testMissingAllRequired() {
        Map<String, String> envVars = new HashMap<>();

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars));
        assertThat(e.getMessage(), is("The STRIMZI_LEADER_ELECTION_LEASE_NAME, STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE and STRIMZI_LEADER_ELECTION_IDENTITY options are required and have to be set when leader election is enabled."));
    }

    @Test
    public void testMissingSomeRequired() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE, "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY, "my-pod");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars));
        assertThat(e.getMessage(), is("The STRIMZI_LEADER_ELECTION_LEASE_NAME, STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE and STRIMZI_LEADER_ELECTION_IDENTITY options are required and have to be set when leader election is enabled."));

        Map<String, String> envVars2 = new HashMap<>();
        envVars2.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME, "my-lease");
        envVars2.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY, "my-pod");

        e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars2));
        assertThat(e.getMessage(), is("The STRIMZI_LEADER_ELECTION_LEASE_NAME, STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE and STRIMZI_LEADER_ELECTION_IDENTITY options are required and have to be set when leader election is enabled."));

        Map<String, String> envVars3 = new HashMap<>();
        envVars3.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME, "my-lease");
        envVars3.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE, "my-namespace");

        e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars3));
        assertThat(e.getMessage(), is("The STRIMZI_LEADER_ELECTION_LEASE_NAME, STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE and STRIMZI_LEADER_ELECTION_IDENTITY options are required and have to be set when leader election is enabled."));
    }
}

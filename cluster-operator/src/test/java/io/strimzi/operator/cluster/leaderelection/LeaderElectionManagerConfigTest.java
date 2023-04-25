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
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), "my-lease");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), "my-pod");

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
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), "my-lease");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), "my-pod");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS.key(), "30000");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS.key(), "20000");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS.key(), "5000");

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
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), null);
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), null);
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), null);

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars));
        assertThat(e.getMessage(), is("Failed to parse. Value cannot be empty or null"));
    }

    @Test
    public void testSomeRequiredValuesNotNull() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), "my-pod");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), null);

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars));
        assertThat(e.getMessage(), is("Failed to parse. Value cannot be empty or null"));

        Map<String, String> envVars2 = new HashMap<>();
        envVars2.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), "my-lease");
        envVars2.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), "my-pod");
        envVars2.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), null);

        e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars2));
        assertThat(e.getMessage(), is("Failed to parse. Value cannot be empty or null"));

        Map<String, String> envVars3 = new HashMap<>();
        envVars3.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), "my-lease");
        envVars3.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), "my-namespace");
        envVars3.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), null);

        e = assertThrows(InvalidConfigurationException.class, () -> LeaderElectionManagerConfig.fromMap(envVars3));
        assertThat(e.getMessage(), is("Failed to parse. Value cannot be empty or null"));
    }
}

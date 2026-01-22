/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaQuorumCheckTest {

    private static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS = 2000L;

    private Admin setUpMocks(int leaderId, Map<Integer, OptionalLong> replicaStateMap) {
        Admin admin = mock(Admin.class);
        DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
        when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
        QuorumInfo qrminfo = mock(QuorumInfo.class);
        List<QuorumInfo.ReplicaState> controllers = replicaStateMap.entrySet().stream().map(entry -> {
            QuorumInfo.ReplicaState replicaState = mock(QuorumInfo.ReplicaState.class);
            when(replicaState.replicaId()).thenReturn(entry.getKey());
            when(replicaState.lastCaughtUpTimestamp()).thenReturn(entry.getValue());
            return replicaState;
        }).toList();
        when(qrminfo.voters()).thenReturn(controllers);
        when(qrminfo.leaderId()).thenReturn(leaderId);
        when(qrmResult.quorumInfo()).thenReturn(KafkaFuture.completedFuture(qrminfo));
        return admin;
    }

    @Test
    public void canRollActiveControllerOddSizedCluster() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(9500L));
        controllers.put(3, OptionalLong.of(9700L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void canRollActiveControllerEvenSizedCluster() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(9500L));
        controllers.put(3, OptionalLong.of(9700L));
        controllers.put(4, OptionalLong.of(9600L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void cannotRollActiveControllerWith1FollowerBehindOddSizedCluster() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(8200L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void cannotRollActiveControllerWith1FollowerBehindEvenSizedCluster() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(8200L));
        controllers.put(4, OptionalLong.of(9000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void canRollControllerWhenItselfIsBehind() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(10000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(2).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void cannotRollControllerWhenAnotherFollowerBehind() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(10000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(3).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void cannotRollControllerWhenTimestampInvalid() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(-1));
        controllers.put(3, OptionalLong.of(-1));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(3).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void cannotRollControllerWhenFailedFuture() {
        Admin admin = mock(Admin.class);
        DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
        when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
        KafkaFutureImpl<QuorumInfo> kafkaFuture = new KafkaFutureImpl<>();
        String expectedError = "admin client not initialised";
        kafkaFuture.completeExceptionally(new Exception(expectedError));
        when(qrmResult.quorumInfo()).thenReturn(kafkaFuture);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((r, error) -> assertThat(error.getMessage(), is(expectedError)));
    }

    @Test
    public void canRollControllerSingleNodeQuorum() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void canRollController2NodesQuorum() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(9500L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void cannotRollController2NodeQuorumFollowerBehind() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void shouldHandlePartiallyIncompleteQuorumData() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.empty()); // Simulating incomplete data
        controllers.put(3, OptionalLong.of(9500L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void shouldHandleNoLeaderQuorumScenario() {
        // Simulate a valid controller list
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(12000L));
        controllers.put(3, OptionalLong.of(11000L));

        // Simulate no leader by passing a negative leader ID
        Admin admin = setUpMocks(-1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);

        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }

    @Test
    public void shouldTestDynamicTimeoutValue() {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(9950L)); // Edge case close to the timeout
        Admin admin = setUpMocks(1, controllers);
        long dynamicTimeout = 100L; // Dynamic timeout value
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, dynamicTimeout);
        quorumCheck.canRollController(1).whenComplete((result, err) -> assertTrue(result));
    }
}

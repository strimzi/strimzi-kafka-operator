/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class KafkaQuorumCheckTest {

    private static Vertx vertx;
    private static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS = 2000L;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

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
    public void canRollActiveController(VertxTestContext context) {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(9500L));
        controllers.put(3, OptionalLong.of(9700L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertTrue(result));
            context.completeNow();
        }));

    }

    @Test
    public void canRollActiveControllerWith1FollowerBehind(VertxTestContext context) {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(8200L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertTrue(result));
            context.completeNow();
        }));
    }

    @Test
    public void cannotRollActiveControllersMostFollowersBehind(VertxTestContext context) {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(8000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertFalse(result));
            context.completeNow();
        }));
    }

    @Test
    public void canRollControllerWhenItselfIsBehind(VertxTestContext context) {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(10000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(2).onComplete(context.succeeding(result -> {
            context.verify(() -> assertTrue(result));
            context.completeNow();
        }));
    }

    @Test
    public void cannotRollControllerWhenAnotherFollowerBehind(VertxTestContext context) {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(7000L));
        controllers.put(3, OptionalLong.of(10000L));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(3).onComplete(context.succeeding(result -> {
            context.verify(() -> assertFalse(result));
            context.completeNow();
        }));
    }

    @Test
    public void cannotRollControllerWhenTimestampInvalid(VertxTestContext context) {
        Map<Integer, OptionalLong> controllers = new HashMap<>();
        controllers.put(1, OptionalLong.of(10000L));
        controllers.put(2, OptionalLong.of(-1));
        controllers.put(3, OptionalLong.of(-1));
        Admin admin = setUpMocks(1, controllers);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(3).onComplete(context.succeeding(result -> {
            context.verify(() -> assertFalse(result));
            context.completeNow();
        }));
    }

    @Test
    public void cannotRollControllerWhenFailedFuture(VertxTestContext context) {
        Admin admin = mock(Admin.class);
        DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
        when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
        KafkaFutureImpl<QuorumInfo> kafkaFuture = new KafkaFutureImpl<>();
        String expectedError = "admin client not initialised";
        kafkaFuture.completeExceptionally(new Exception(expectedError));
        when(qrmResult.quorumInfo()).thenReturn(kafkaFuture);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin, vertx, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS);
        quorumCheck.canRollController(1).onComplete(context.failing(error -> {
            context.verify(() -> assertThat(error.getMessage(), is(expectedError)));
            context.completeNow();
        }));
    }

}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@ExtendWith(VertxExtension.class)
public class KafkaUserQuotasIT {

    private static ZkClient zkClient;

    private static KafkaUserQuotasOperator kuq;

    private KafkaUserQuotas defaultQuotas;

    private static Vertx vertx;

    private static KafkaCluster kafkaCluster;

    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();

        try {
            kafkaCluster =
                    new KafkaCluster()
                            .usingDirectory(Testing.Files.createTestingDirectory("user-quotas-operator-integration-test"))
                            .deleteDataPriorToStartup(true)
                            .deleteDataUponShutdown(true)
                            .addBrokers(1)
                            .startup();
        } catch (IOException e) {
            assertThat(false, is(true));
        }

        zkClient = new ZkClient("localhost:" + kafkaCluster.zkPort(), 6000_0, 30_000, new BytesPushThroughSerializer());

        kuq = new KafkaUserQuotasOperator(vertx,
                new DefaultAdminClientProvider().createAdminClient(kafkaCluster.brokerList(), null, null, null));
    }

    @AfterAll
    public static void afterAll() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        if (vertx != null) {
            vertx.close();
        }
    }

    @BeforeEach
    public void beforeEach() {
        defaultQuotas = new KafkaUserQuotas();
        defaultQuotas.setConsumerByteRate(1000);
        defaultQuotas.setProducerByteRate(2000);
    }

    @Test
    public void testTlsUserExistsAfterCreate() throws Exception {
        testUserExistsAfterCreate("CN=userExists");
    }

    @Test
    public void testRegularUserExistsAfterCreate() throws Exception {
        testUserExistsAfterCreate("userExists");
    }

    public void testUserExistsAfterCreate(String username) throws Exception {
        assertThat(kuq.exists(username), is(false));
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(kuq.exists(username), is(true));
    }

    @Test
    public void testTlsUserDoesNotExistPriorToCreate() throws Exception {
        testUserDoesNotExistPriorToCreate("CN=userNotExists");
    }

    @Test
    public void testRegularUserDoesNotExistPriorToCreate() throws Exception {
        testUserDoesNotExistPriorToCreate("userNotExists");
    }

    public void testUserDoesNotExistPriorToCreate(String username) throws Exception {
        assertThat(kuq.exists(username), is(false));
    }

    @Test
    public void testCreateOrUpdateTlsUser() throws Exception {
        testUserQuotasNotExist("CN=tlsUser");
        testCreateOrUpdate("CN=tlsUser");
    }

    @Test
    public void testCreateOrUpdateRegularUser() throws Exception {
        testUserQuotasNotExist("user");
        testCreateOrUpdate("user");
    }

    @Test
    public void testCreateOrUpdateScramShaUser() throws Exception {
        createScramShaUser("scramShaUser", "scramShaPassword");
        testCreateOrUpdate("scramShaUser");
    }

    public void testCreateOrUpdate(String username) throws Exception {
        KafkaUserQuotas newQuotas = new KafkaUserQuotas();
        newQuotas.setConsumerByteRate(1000);
        newQuotas.setProducerByteRate(2000);
        kuq.createOrUpdate(username, newQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        testDescribeUserQuotas(username, newQuotas);
    }

    @Test
    public void testCreateOrUpdateTwiceTlsUser() throws Exception {
        testCreateOrUpdateTwice("CN=doubleCreate");
    }

    @Test
    public void testCreateOrUpdateTwiceRegularUSer() throws Exception {
        testCreateOrUpdateTwice("doubleCreate");
    }

    public void testCreateOrUpdateTwice(String username) throws Exception {
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(false));
        assertThat(kuq.describeUserQuotas(username), is(nullValue()));

        kuq.createOrUpdate(username, defaultQuotas);
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        testDescribeUserQuotas(username, defaultQuotas);
    }

    @Test
    public void testDeleteTlsUser() throws Exception {
        testDelete("CN=normalDelete");
    }

    @Test
    public void testDeleteRegularUser() throws Exception {
        testDelete("normalDelete");
    }

    public void testDelete(String username) throws Exception {
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        assertThat(kuq.exists(username), is(true));

        kuq.delete(username);
        assertThat(kuq.exists(username), is(false));
    }

    @Test
    public void testDeleteTwiceTlsUser() throws Exception {
        testDeleteTwice("CN=doubleDelete");
    }

    @Test
    public void testDeleteTwiceRegularUser() throws Exception {
        testDeleteTwice("doubleDelete");
    }

    public void testDeleteTwice(String username) throws Exception {
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        assertThat(kuq.exists(username), is(true));

        kuq.delete(username);
        kuq.delete(username);
        assertThat(kuq.exists(username), is(false));
    }

    @Test
    public void testUpdateConsumerByteRate() throws Exception {
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        defaultQuotas.setConsumerByteRate(4000);
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        assertThat(kuq.describeUserQuotas("changeProducerByteRate").getConsumerByteRate(), is(4000));
    }

    @Test
    public void testUpdateProducerByteRate() throws Exception {
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        defaultQuotas.setProducerByteRate(8000);
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        assertThat(kuq.describeUserQuotas("changeProducerByteRate").getProducerByteRate(), is(8000));
    }

    @Test
    public void testUserQuotasToClientQuotaAlterationOps() {
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2000);
        quotas.setProducerByteRate(4000);
        quotas.setRequestPercentage(40);
        Set<ClientQuotaAlteration.Op> ops = kuq.toClientQuotaAlterationOps(quotas);
        assertThat(ops, hasSize(3));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("consumer_byte_rate", 2000d)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("producer_byte_rate", 4000d)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("request_percentage", 40d)), is(true));

        quotas.setConsumerByteRate(null);
        quotas.setProducerByteRate(null);
        quotas.setRequestPercentage(null);
        ops = kuq.toClientQuotaAlterationOps(quotas);
        assertThat(ops, hasSize(3));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("consumer_byte_rate", null)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("producer_byte_rate", null)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("request_percentage", null)), is(true));
    }

    @Test
    public void testClientQuotaAlterationOpsToUserQuotas() {
        Map<String, Double> map = new HashMap<>(3);
        map.put("consumer_byte_rate", 2000d);
        map.put("producer_byte_rate", 4000d);
        map.put("request_percentage", 40d);
        KafkaUserQuotas quotas = kuq.fromClientQuota(map);
        assertThat(quotas.getConsumerByteRate(), is(2000));
        assertThat(quotas.getProducerByteRate(), is(4000));
        assertThat(quotas.getRequestPercentage(), is(40));

        map.remove("consumer_byte_rate");
        map.remove("producer_byte_rate");
        map.remove("request_percentage");
        quotas = kuq.fromClientQuota(map);
        assertThat(quotas.getConsumerByteRate(), is(nullValue()));
        assertThat(quotas.getProducerByteRate(), is(nullValue()));
        assertThat(quotas.getRequestPercentage(), is(nullValue()));
    }

    @Test
    public void testReconcileCreatesTlsUserWithQuotas(VertxTestContext testContext) throws Exception  {
        testReconcileCreatesUserWithQuotas("CN=createTestUser", testContext);
    }

    @Test
    public void testReconcileCreatesRegularUserWithQuotas(VertxTestContext testContext) throws Exception  {
        testReconcileCreatesUserWithQuotas("createTestUser", testContext);
    }

    public void testReconcileCreatesUserWithQuotas(String username, VertxTestContext testContext) throws Exception  {
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2_000_000);
        quotas.setProducerByteRate(1_000_000);
        quotas.setRequestPercentage(50);

        assertThat(kuq.exists(username), is(false));

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, quotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(true));
                assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
                testDescribeUserQuotas(username, quotas);
                async.flag();
            })));
    }

    @Test
    public void testReconcileUpdatesTlsUserQuotaValues(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotaValues("CN=updateTestUser", testContext);
    }

    @Test
    public void testReconcileUpdatesRegularUserQuotaValues(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotaValues("updateTestUser", testContext);
    }

    public void testReconcileUpdatesUserQuotaValues(String username, VertxTestContext testContext) throws Exception {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(username, initialQuotas);
        assertThat(kuq.exists(username), is(true));
        testDescribeUserQuotas(username, initialQuotas);

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);
        updatedQuotas.setRequestPercentage(75);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, updatedQuotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(true));
                assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
                testDescribeUserQuotas(username, updatedQuotas);
                async.flag();
            })));
    }

    @Test
    public void testReconcileUpdatesTlsUserQuotasWithFieldRemovals(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotasWithFieldRemovals("CN=updateTestUser", testContext);
    }

    @Test
    public void testReconcileUpdatesRegularUserQuotasWithFieldRemovals(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotasWithFieldRemovals("updateTestUser", testContext);
    }

    public void testReconcileUpdatesUserQuotasWithFieldRemovals(String username, VertxTestContext testContext) throws Exception {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(username, initialQuotas);
        assertThat(kuq.exists(username), is(true));
        testDescribeUserQuotas(username, initialQuotas);

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, updatedQuotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(true));
                assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
                testDescribeUserQuotas(username, updatedQuotas);
                async.flag();
            })));

    }

    @Test
    public void testReconcileDeletesTlsUserForNullQuota(VertxTestContext testContext) throws Exception {
        testReconcileDeletesUserForNullQuota("CN=deleteTestUser", testContext);
    }

    @Test
    public void testReconcileDeletesRegularUserForNullQuota(VertxTestContext testContext) throws Exception {
        testReconcileDeletesUserForNullQuota("deleteTestUser", testContext);
    }

    public void testReconcileDeletesUserForNullQuota(String username, VertxTestContext testContext) throws Exception {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(username, initialQuotas);
        assertThat(kuq.exists(username), is(true));
        testDescribeUserQuotas(username, initialQuotas);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, null)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(false));
                async.flag();
            })));
    }

    private boolean isPathExist(String path) {
        return zkClient.exists(path);
    }

    private String encodeUsername(String username) {
        try {
            return URLEncoder.encode(username, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to encode username", e);
        }
    }

    private void createScramShaUser(String username, String password) {
        // creating SCRAM-SHA user upfront to check it works because it shares same path in ZK as quotas
        ScramShaCredentials scramShaCred = new ScramShaCredentials("localhost:" + kafkaCluster.zkPort(), 6_000);
        scramShaCred.createOrUpdate(username, password);
        assertThat(scramShaCred.exists(username), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/" + username), is(true));
    }

    private void testUserQuotasNotExist(String username) throws Exception {
        assertThat(kuq.describeUserQuotas(username), is(nullValue()));
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(false));
    }

    private void testDescribeUserQuotas(String username, KafkaUserQuotas quotas) throws Exception {
        assertThat(kuq.describeUserQuotas(username), is(notNullValue()));
        assertThat(kuq.describeUserQuotas(username).getConsumerByteRate(), is(quotas.getConsumerByteRate()));
        assertThat(kuq.describeUserQuotas(username).getProducerByteRate(), is(quotas.getProducerByteRate()));
        assertThat(kuq.describeUserQuotas(username).getRequestPercentage(), is(quotas.getRequestPercentage()));
    }
}

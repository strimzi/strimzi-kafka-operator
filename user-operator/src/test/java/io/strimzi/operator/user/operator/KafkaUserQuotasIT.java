/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(VertxExtension.class)
public class KafkaUserQuotasIT {

    private static EmbeddedZooKeeper zkServer;

    private static KafkaUserQuotasOperator kuq;

    private KafkaUserQuotas defaultQuotas;

    private static Vertx vertx;


    @BeforeAll
    public static void before() throws IOException, InterruptedException {
        vertx = Vertx.vertx();
        // Start ZookKeeper Server
        zkServer = new EmbeddedZooKeeper();
        kuq = new KafkaUserQuotasOperator(vertx, zkServer.getZkConnectString(), 6_000);
    }

    @AfterAll
    public static void after() {
        vertx.close();
        // Teardown ZooKeeper Server
        zkServer.close();
    }

    @BeforeEach
    public void beforeEach() {
        defaultQuotas = new KafkaUserQuotas();
        defaultQuotas.setConsumerByteRate(1000);
        defaultQuotas.setProducerByteRate(2000);
    }

    @Test
    public void testTlsUserExistsAfterCreate() {
        testUserExistsAfterCreate("CN=userExists");
    }

    @Test
    public void testRegularUserExistsAfterCreate() {
        testUserExistsAfterCreate("userExists");
    }

    public void testUserExistsAfterCreate(String username) {
        assertThat(kuq.exists(username), is(false));
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(kuq.exists(username), is(true));
    }

    @Test
    public void testTlsUserDoesNotExistPriorToCreate() {
        testUserDoesNotExistPriorToCreate("CN=userNotExists");
    }

    @Test
    public void testRegularUserDoesNotExistPriorToCreate() {
        testUserDoesNotExistPriorToCreate("userNotExists");
    }

    public void testUserDoesNotExistPriorToCreate(String username) {
        assertThat(kuq.exists(username), is(false));
    }

    @Test
    public void testCreateOrUpdateTlsUser() {
        testCreateOrUpdate("CN=tlsUser");
    }

    @Test
    public void testCreateOrUpdateRegularUser() {
        testCreateOrUpdate("user");
    }

    public void testCreateOrUpdate(String username) {
        assertThat(kuq.exists(username), is(false));
        assertThat(kuq.getQuotas(username), is(nullValue()));
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(false));

        KafkaUserQuotas newQuotas = new KafkaUserQuotas();
        newQuotas.setConsumerByteRate(1000);
        newQuotas.setProducerByteRate(2000);
        kuq.createOrUpdate(username, newQuotas);
        assertThat(kuq.exists(username), is(true));
        assertThat(kuq.getQuotas(username).getJsonObject("config").getString("consumer_byte_rate"), is("1000"));
        assertThat(kuq.getQuotas(username).getJsonObject("config").getString("producer_byte_rate"), is("2000"));
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
    }

    @Test
    public void testCreateOrUpdateTwiceTlsUser() {
        testCreateOrUpdateTwice("CN=doubleCreate");
    }

    @Test
    public void testCreateOrUpdateTwiceRegularUSer() {
        testCreateOrUpdateTwice("doubleCreate");
    }

    public void testCreateOrUpdateTwice(String username) {
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(false));
        assertThat(kuq.exists(username), is(false));
        assertThat(kuq.getQuotas(username), is(nullValue()));

        kuq.createOrUpdate(username, defaultQuotas);
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(kuq.exists(username), is(true));
        assertThat(kuq.getQuotas(username).getJsonObject("config").getString("consumer_byte_rate"), is("1000"));
        assertThat(kuq.getQuotas(username).getJsonObject("config").getString("producer_byte_rate"), is("2000"));
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
    }

    @Test
    public void testDeleteTlsUser() {
        testDelete("CN=normalDelete");
    }

    @Test
    public void testDeleteRegularUser() {
        testDelete("normalDelete");
    }

    public void testDelete(String username) {
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
        assertThat(kuq.exists(username), is(true));

        kuq.delete(username);
        assertThat(kuq.exists(username), is(false));
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(false));
    }

    @Test
    public void testDeleteTwiceTlsUser() {
        testDeleteTwice("CN=doubleDelete");
    }

    @Test
    public void testDeleteTwiceRegularUser() {
        testDeleteTwice("doubleDelete");
    }

    public void testDeleteTwice(String username) {
        kuq.createOrUpdate(username, defaultQuotas);
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
        assertThat(kuq.exists(username), is(true));

        kuq.delete(username);
        kuq.delete(username);
        assertThat(kuq.exists(username), is(false));
        assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(false));
    }

    @Test
    public void testUpdateConsumerByteRate() {
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        defaultQuotas.setConsumerByteRate(4000);
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        assertThat(kuq.getQuotas("changeProducerByteRate").getJsonObject("config").getString("consumer_byte_rate"),
                is("4000"));
    }

    @Test
    public void testUpdateProducerByteRate() {
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        defaultQuotas.setProducerByteRate(8000);
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        assertThat(kuq.getQuotas("changeProducerByteRate").getJsonObject("config").getString("producer_byte_rate"),
                is("8000"));
    }


    @Test
    public void testValidation()    {
        JsonObject valid = new JsonObject().put("version", 1);
        JsonObject invalidEmptyJsonObject = new JsonObject();
        JsonObject invalidVersion = new JsonObject().put("version", 2);

        kuq.validateJsonVersion(valid);

        assertThrows(RuntimeException.class, () -> kuq.validateJsonVersion(invalidEmptyJsonObject),
                "Empty JsonObject should cause validate to throw Exception");

        assertThrows(RuntimeException.class, () -> kuq.validateJsonVersion(invalidVersion),
                "Invalid version (!=1) should cause validate to throw Exception");
    }

    @Test
    public void testRemoveQuotasFromJsonUserWorksForVariousJonObjectInitialisations()  {
        JsonObject original = new JsonObject(new String(kuq.createUserJson(defaultQuotas), StandardCharsets.UTF_8));
        JsonObject updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("producer_byte_rate", "1000").put("consumer_byte_rate", "2000"));
        updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("consumer_byte_rate", "1000"));
        updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));
    }

    @Test
    public void testRemoveQuotasFromJsonUserPersistsNonQuotaKeys()  {
        JsonObject original = new JsonObject(new String(kuq.createUserJson(defaultQuotas), StandardCharsets.UTF_8));
        String keyThatShouldPersist = "persist";
        int valueThatShouldPersist = 42;
        original.getJsonObject("config").put(keyThatShouldPersist, valueThatShouldPersist);

        JsonObject updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getInteger(keyThatShouldPersist), is(valueThatShouldPersist));
    }

    @Test
    public void testCreateOrUpdateUserJsonByUpdatingAndRemovingFields()  {
        // test creation
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2000);
        quotas.setProducerByteRate(4000);
        quotas.setRequestPercentage(40);

        JsonObject createdUserJson = new JsonObject(new String(kuq.createUserJson(quotas), StandardCharsets.UTF_8));
        assertThat(createdUserJson.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(createdUserJson.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(createdUserJson.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(createdUserJson.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));
        assertThat(createdUserJson.getJsonObject("config").getString("producer_byte_rate"), is("4000"));
        assertThat(createdUserJson.getJsonObject("config").getString("request_percentage"), is("40"));


        byte[] createdUserJsonInBytes = createdUserJson.encode().getBytes(StandardCharsets.UTF_8);

        // test update by removing request_percentage field
        KafkaUserQuotas quotas2 = new KafkaUserQuotas();
        quotas2.setConsumerByteRate(2000);
        quotas2.setProducerByteRate(4000);

        JsonObject updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdUserJsonInBytes, quotas2), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("4000"));

        // test update by removing producer_byte_rate field
        KafkaUserQuotas quotas3 = new KafkaUserQuotas();
        quotas3.setConsumerByteRate(2000);
        quotas3.setRequestPercentage(40);

        updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdUserJsonInBytes, quotas3), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is("40"));

        // test update by removing consumer_byte_rate field
        KafkaUserQuotas quotas4 = new KafkaUserQuotas();
        quotas4.setProducerByteRate(4000);
        quotas4.setRequestPercentage(40);

        updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdUserJsonInBytes, quotas4), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("4000"));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is("40"));

        // test update by modifying all fields
        KafkaUserQuotas quotas5 = new KafkaUserQuotas();
        quotas5.setConsumerByteRate(20000);
        quotas5.setProducerByteRate(40000);
        quotas5.setRequestPercentage(50);

        updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdUserJsonInBytes, quotas5), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("20000"));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("40000"));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is("50"));
    }

    @Test
    public void testReconcileCreatesTlsUserWithQuotas(VertxTestContext testContext)  {
        testReconcileCreatesUserWithQuotas("CN=createTestUser", testContext);
    }

    @Test
    public void testReconcileCreatesRegularUserWithQuotas(VertxTestContext testContext)  {
        testReconcileCreatesUserWithQuotas("createTestUser", testContext);
    }

    public void testReconcileCreatesUserWithQuotas(String username, VertxTestContext testContext)  {
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2_000_000);
        quotas.setProducerByteRate(1_000_000);
        quotas.setRequestPercentage(50);

        assertThat(kuq.exists(username), is(false));

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, quotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(true));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("consumer_byte_rate"), is("2000000"));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("producer_byte_rate"), is("1000000"));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("request_percentage"), is("50"));
                assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
                async.flag();
            })));
    }

    @Test
    public void testReconcileUpdatesTlsUserQuotaValues(VertxTestContext testContext)  {
        testReconcileUpdatesUserQuotaValues("CN=updateTestUser", testContext);
    }

    @Test
    public void testReconcileUpdatesRegularUserQuotaValues(VertxTestContext testContext)  {
        testReconcileUpdatesUserQuotaValues("updateTestUser", testContext);
    }

    public void testReconcileUpdatesUserQuotaValues(String username, VertxTestContext testContext)  {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(username, initialQuotas);
        assertThat(kuq.exists(username), is(true));

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);
        updatedQuotas.setRequestPercentage(75);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, updatedQuotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(true));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("consumer_byte_rate"), is("4000000"));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("producer_byte_rate"), is("3000000"));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("request_percentage"), is("75"));
                assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
                async.flag();
            })));
    }

    @Test
    public void testReconcileUpdatesTlsUserQuotasWithFieldRemovals(VertxTestContext testContext)  {
        testReconcileUpdatesUserQuotasWithFieldRemovals("CN=updateTestUser", testContext);
    }

    @Test
    public void testReconcileUpdatesRegularUserQuotasWithFieldRemovals(VertxTestContext testContext)  {
        testReconcileUpdatesUserQuotasWithFieldRemovals("updateTestUser", testContext);
    }

    public void testReconcileUpdatesUserQuotasWithFieldRemovals(String username, VertxTestContext testContext)  {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(username, initialQuotas);
        assertThat(kuq.exists(username), is(true));

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, updatedQuotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(true));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("consumer_byte_rate"), is("4000000"));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("producer_byte_rate"), is("3000000"));
                assertThat(kuq.getQuotas(username).getJsonObject("config").getString("request_percentage"), is(nullValue()));
                assertThat(kuq.isPathExist("/config/users/" + kuq.encodeUsername(username)), is(true));
                async.flag();
            })));

    }

    @Test
    public void testReconcileDeletesTlsUserForNullQuota(VertxTestContext testContext)  {
        testReconcileDeletesUserForNullQuota("CN=deleteTestUser", testContext);
    }

    @Test
    public void testReconcileDeletesRegularUserForNullQuota(VertxTestContext testContext)  {
        testReconcileDeletesUserForNullQuota("deleteTestUser", testContext);
    }

    public void testReconcileDeletesUserForNullQuota(String username, VertxTestContext testContext)  {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(username, initialQuotas);
        assertThat(kuq.exists(username), is(true));

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(username, null)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(username), is(false));
                async.flag();
            })));
    }

    @Test
    public void testEncodeUser()    {
        assertThat(KafkaUserQuotasOperator.encodeUsername("jack"), is("jack"));
        assertThat(KafkaUserQuotasOperator.encodeUsername("CN=grealish"), is("CN%3Dgrealish"));
    }
}

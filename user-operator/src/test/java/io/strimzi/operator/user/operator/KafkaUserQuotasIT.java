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
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(VertxExtension.class)
public class KafkaUserQuotasIT {

    private static EmbeddedZooKeeper zkServer;

    private KafkaUserQuotasOperator kuq;

    private KafkaUserQuotas defaultQuotas;

    private static Vertx vertx;


    @BeforeAll
    public static void startZk() throws IOException, InterruptedException {
        vertx = Vertx.vertx();
        zkServer = new EmbeddedZooKeeper();
    }

    @AfterAll
    public static void stopZk() {
        vertx.close();
        zkServer.close();
    }

    @BeforeEach
    public void createKUQ() {
        defaultQuotas = new KafkaUserQuotas();
        defaultQuotas.setConsumerByteRate(1000);
        defaultQuotas.setProducerByteRate(2000);
        kuq = new KafkaUserQuotasOperator(vertx, zkServer.getZkConnectString(), 6_000);
    }

    @Test
    public void normalCreate() {
        assertThat(kuq.exists("normalCreate"), is(false));
        assertThat(kuq.getQuotas("normalCreate"), is(nullValue()));
        assertThat(kuq.isPathExist("/config/users/normalCreate"), is(false));
        KafkaUserQuotas newQuotas = new KafkaUserQuotas();
        newQuotas.setConsumerByteRate(1000);
        newQuotas.setProducerByteRate(2000);
        kuq.createOrUpdate("normalCreate", newQuotas);
        assertThat(kuq.exists("normalCreate"), is(true));
        assertThat(kuq.getQuotas("normalCreate").getJsonObject("config").getString("consumer_byte_rate"), is("1000"));
        assertThat(kuq.getQuotas("normalCreate").getJsonObject("config").getString("producer_byte_rate"), is("2000"));
        assertThat(kuq.isPathExist("/config/users/normalCreate"), is(true));
    }

    @Test
    public void doubleCreate() {
        assertThat(kuq.isPathExist("/config/users/doublelCreate"), is(false));
        assertThat(kuq.exists("doubleCreate"), is(false));
        assertThat(kuq.getQuotas("doubleCreate"), is(nullValue()));
        kuq.createOrUpdate("doubleCreate", defaultQuotas);
        kuq.createOrUpdate("doubleCreate", defaultQuotas);
        assertThat(kuq.exists("doubleCreate"), is(true));
        assertThat(kuq.getQuotas("doubleCreate").getJsonObject("config").getString("consumer_byte_rate"), is("1000"));
        assertThat(kuq.getQuotas("doubleCreate").getJsonObject("config").getString("producer_byte_rate"), is("2000"));
        assertThat(kuq.isPathExist("/config/users/doubleCreate"), is(true));
    }

    @Test
    public void normalDelete() {
        kuq.createOrUpdate("normalDelete", defaultQuotas);
        assertThat(kuq.isPathExist("/config/users/normalDelete"), is(true));
        assertThat(kuq.exists("normalDelete"), is(true));
        kuq.delete("normalDelete");
        assertThat(kuq.exists("normalDelete"), is(false));
        assertThat(kuq.isPathExist("/config/users/normalDelete"), is(false));
    }

    @Test
    public void doubleDelete() {
        kuq.createOrUpdate("doubleDelete", defaultQuotas);
        assertThat(kuq.isPathExist("/config/users/doubleDelete"), is(true));
        assertThat(kuq.exists("doubleDelete"), is(true));
        kuq.delete("doubleDelete");
        kuq.delete("doubleDelete");
        assertThat(kuq.exists("doubleDelete"), is(false));
        assertThat(kuq.isPathExist("/config/users/doubleDelete"), is(false));
    }

    @Test
    public void changeProducerByteRate() {
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
        defaultQuotas.setProducerByteRate(8000);
        kuq.createOrUpdate("changeProducerByteRate", defaultQuotas);
    }

    @Test
    public void userExists() {
        kuq.createOrUpdate("userExists", defaultQuotas);
        assertThat(kuq.exists("userExists"), is(true));
    }

    @Test
    public void userNotExists() {
        assertThat(kuq.exists("userNotExists"), is(false));
    }


    @Test
    public void testValidation()    {
        JsonObject valid = new JsonObject().put("version", 1);
        JsonObject invalid1 = new JsonObject();
        JsonObject invalid2 = new JsonObject().put("version", 2);

        kuq.validateJsonVersion(valid);

        try {
            kuq.validateJsonVersion(invalid1);
            fail("Invalid Json 1 didn't raised exception");
        } catch (RuntimeException e)    {
            // noop
        }

        try {
            kuq.validateJsonVersion(invalid2);
            fail("Invalid Json 2 didn't raised exception");
        } catch (RuntimeException e)    {
            // noop
        }
    }

    @Test
    public void testDeletion()  {
        JsonObject original = new JsonObject(new String(kuq.createUserJson(defaultQuotas), StandardCharsets.UTF_8));
        original.getJsonObject("config").put("persist", 42);

        JsonObject updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getInteger("persist"), is(42));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("producer_byte_rate", "1000").put("consumer_byte_rate", "2000"));
        updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("consumer_byte_rate", "1000"));
        updated = kuq.removeQuotasFromJsonUser(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));

    }

    @Test
    public void testUpdateAndFieldRemoval()  {
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2000);
        quotas.setProducerByteRate(4000);
        quotas.setRequestPercentage(40);

        JsonObject created = new JsonObject(new String(kuq.createUserJson(quotas), StandardCharsets.UTF_8));
        assertThat(created.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(created.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(created.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(created.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));
        assertThat(created.getJsonObject("config").getString("producer_byte_rate"), is("4000"));
        assertThat(created.getJsonObject("config").getString("request_percentage"), is("40"));

        byte[] createdBytes = created.encode().getBytes(StandardCharsets.UTF_8);

        KafkaUserQuotas quotas2 = new KafkaUserQuotas();
        quotas2.setConsumerByteRate(2000);
        quotas2.setProducerByteRate(4000);

        JsonObject updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdBytes, quotas2), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("4000"));

        KafkaUserQuotas quotas3 = new KafkaUserQuotas();
        quotas3.setConsumerByteRate(2000);
        quotas3.setRequestPercentage(40);

        updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdBytes, quotas3), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is("40"));

        KafkaUserQuotas quotas4 = new KafkaUserQuotas();
        quotas4.setProducerByteRate(4000);
        quotas4.setRequestPercentage(40);

        updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdBytes, quotas4), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("4000"));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is("40"));

        KafkaUserQuotas quotas5 = new KafkaUserQuotas();
        quotas5.setConsumerByteRate(20000);
        quotas5.setProducerByteRate(40000);
        quotas5.setRequestPercentage(50);

        updated = new JsonObject(new String(kuq.createOrUpdateUserJson(createdBytes, quotas5), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("20000"));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("40000"));
        assertThat(updated.getJsonObject("config").getString("request_percentage"), is("50"));
    }

    @Test
    public void testReconcileCreate(VertxTestContext testContext)  {
        String user = "createTestUser";
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2_000_000);
        quotas.setProducerByteRate(1_000_000);
        quotas.setRequestPercentage(50);

        testContext.verify(() -> assertThat(kuq.exists(user), is(false)));

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(user, quotas)
                .setHandler(testContext.succeeding(res -> {
                    testContext.verify(() -> {
                        assertThat(kuq.exists(user), is(true));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("consumer_byte_rate"), is("2000000"));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("producer_byte_rate"), is("1000000"));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("request_percentage"), is("50"));
                        assertThat(kuq.isPathExist("/config/users/" + user), is(true));
                    });

                    async.flag();
                }));
    }

    @Test
    public void testReconcileUpdate(VertxTestContext testContext)  {
        String user = "updateTestUser";
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(user, initialQuotas);
        testContext.verify(() -> assertThat(kuq.exists(user), is(true)));

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);
        updatedQuotas.setRequestPercentage(75);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(user, updatedQuotas)
                .setHandler(testContext.succeeding(res -> {
                    testContext.verify(() -> {
                        assertThat(kuq.exists(user), is(true));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("consumer_byte_rate"), is("4000000"));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("producer_byte_rate"), is("3000000"));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("request_percentage"), is("75"));
                        assertThat(kuq.isPathExist("/config/users/" + user), is(true));
                    });

                    async.flag();
                }));
    }

    @Test
    public void testReconcileUpdateWithRemovedField(VertxTestContext testContext)  {
        String user = "updateTestUser";
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(user, initialQuotas);
        testContext.verify(() -> assertThat(kuq.exists(user), is(true)));

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(user, updatedQuotas)
                .setHandler(testContext.succeeding(res -> {
                    testContext.verify(() -> {
                        assertThat(kuq.exists(user), is(true));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("consumer_byte_rate"), is("4000000"));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("producer_byte_rate"), is("3000000"));
                        assertThat(kuq.getQuotas(user).getJsonObject("config").getString("request_percentage"), is(nullValue()));
                        assertThat(kuq.isPathExist("/config/users/" + user), is(true));
                    });

                    async.flag();
                }));
    }

    @Test
    public void testReconcileDelete(VertxTestContext testContext)  {
        String user = "deleteTestUser";
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);

        kuq.createOrUpdate(user, initialQuotas);
        testContext.verify(() -> assertThat(kuq.exists(user), is(true)));

        Checkpoint async = testContext.checkpoint();
        kuq.reconcile(user, null)
                .setHandler(testContext.succeeding(res -> {
                    testContext.verify(() -> assertThat(kuq.exists(user), is(false)));

                    async.flag();
                }));
    }
}

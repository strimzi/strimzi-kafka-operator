/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

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
        JsonObject original = new JsonObject().put("version", 1).put("config", kuq.quotasToJson(defaultQuotas));
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
    public void testUpdate()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("consumer_byte_rate", "1000"));
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2000);
        JsonObject updated = new JsonObject(new String(kuq.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), quotas), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("2000"));

        quotas.setConsumerByteRate(3000);
        quotas.setProducerByteRate(4000);
        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("consumer_byte_rate", "1000").put("producer_byte_rate", "2000"));
        updated = new JsonObject(new String(kuq.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), quotas), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("producer_byte_rate"), is("4000"));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is("3000"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        quotas.setProducerByteRate(null);
        quotas.setConsumerByteRate(1000);
        updated = new JsonObject(new String(kuq.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), quotas), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("consumer_byte_rate"), is(notNullValue()));
    }
}

/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.test.EmbeddedZooKeeper;
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


public class ScramShaCredentialsTest {

    private static EmbeddedZooKeeper zkServer;

    private ScramShaCredentials scramShaCred;

    @BeforeAll
    public static void startZk() throws IOException, InterruptedException {
        zkServer = new EmbeddedZooKeeper();
    }

    @AfterAll
    public static void stopZk() {
        zkServer.close();
    }

    @BeforeEach
    public void createSTS() {
        scramShaCred = new ScramShaCredentials(zkServer.getZkConnectString(), 6_000);
    }

    @Test
    public void normalCreate() {
        scramShaCred.createOrUpdate("normalCreate", "foo-password");
    }

    @Test
    public void doubleCreate() {
        scramShaCred.createOrUpdate("doubleCreate", "foo-password");
        scramShaCred.createOrUpdate("doubleCreate", "foo-password");
    }

    @Test
    public void normalDelete() {
        scramShaCred.createOrUpdate("normalDelete", "foo-password");
        scramShaCred.delete("normalDelete");
    }

    @Test
    public void doubleDelete() {
        scramShaCred.createOrUpdate("doubleDelete", "foo-password");
        scramShaCred.delete("doubleDelete");
        scramShaCred.delete("doubleDelete");
    }

    @Test
    public void changePassword() {
        scramShaCred.createOrUpdate("changePassword", "changePassword-password");
        scramShaCred.createOrUpdate("changePassword", "changePassword-password2");
    }

    @Test
    public void userExists() {
        scramShaCred.createOrUpdate("userExists", "foo-password");
        assertThat(scramShaCred.exists("userExists"), is(true));

    }

    @Test
    public void userNotExists() {
        assertThat(scramShaCred.exists("userNotExists"), is(false));
    }

    @Test
    public void listSome() {
        scramShaCred.createOrUpdate("listSome", "foo-password");
        assertThat(scramShaCred.list().contains("listSome"), is(true));
    }

    @Test
    public void listNone() {
        for (String user : scramShaCred.list()) {
            scramShaCred.delete(user);
        }
        assertThat(scramShaCred.list().isEmpty(), is(true));
    }

    @Test
    public void testValidation()    {
        JsonObject valid = new JsonObject().put("version", 1);
        JsonObject invalid1 = new JsonObject();
        JsonObject invalid2 = new JsonObject().put("version", 2);

        scramShaCred.validateJsonVersion(valid);

        try {
            scramShaCred.validateJsonVersion(invalid1);
            fail("Invalid Json 1 didn't raised exception");
        } catch (RuntimeException e)    {
            // noop
        }

        try {
            scramShaCred.validateJsonVersion(invalid2);
            fail("Invalid Json 2 didn't raised exception");
        } catch (RuntimeException e)    {
            // noop
        }
    }

    @Test
    public void testDeletion()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        original.getJsonObject("config").put("persist", 42);
        JsonObject updated = scramShaCred.deleteUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getInteger("persist"), is(42));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = scramShaCred.deleteUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = scramShaCred.deleteUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = scramShaCred.deleteUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));
    }

    @Test
    public void testUpdate()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        JsonObject updated = new JsonObject(new String(scramShaCred.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), "password"), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(scramShaCred.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), "password"), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = new JsonObject(new String(scramShaCred.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), "password"), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(scramShaCred.updateUserJson(original.encode().getBytes(StandardCharsets.UTF_8), "password"), StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));
    }
}

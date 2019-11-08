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
import java.nio.charset.Charset;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;


public class ScramShaCredentialsTest {

    private static EmbeddedZooKeeper zkServer;

    private ScramShaCredentials ss;

    @BeforeAll
    public static void startZk() throws IOException, InterruptedException {
        zkServer = new EmbeddedZooKeeper();
    }

    @AfterAll
    public static void stopZk() {
        zkServer.close();
    }

    @BeforeEach
    public void createSS() {
        ss = new ScramShaCredentials(zkServer.getZkConnectString(), 6_000);
    }

    @Test
    public void normalCreate() {
        ss.createOrUpdate("normalCreate", "foo-password");
    }

    @Test
    public void doubleCreate() {
        ss.createOrUpdate("doubleCreate", "foo-password");
        ss.createOrUpdate("doubleCreate", "foo-password");
    }

    @Test
    public void normalDelete() {
        ss.createOrUpdate("normalDelete", "foo-password");
        ss.delete("normalDelete");
    }

    @Test
    public void doubleDelete() {
        ss.createOrUpdate("doubleDelete", "foo-password");
        ss.delete("doubleDelete");
        ss.delete("doubleDelete");
    }

    @Test
    public void changePassword() {
        ss.createOrUpdate("changePassword", "changePassword-password");
        ss.createOrUpdate("changePassword", "changePassword-password2");
    }

    @Test
    public void userExists() {
        ss.createOrUpdate("userExists", "foo-password");
        assertThat(ss.exists("userExists"), is(true));

    }

    @Test
    public void userNotExists() {
        assertThat(ss.exists("userNotExists"), is(false));
    }

    @Test
    public void listSome() {
        ss.createOrUpdate("listSome", "foo-password");
        assertThat(ss.list().contains("listSome"), is(true));
    }

    @Test
    public void listNone() {
        for (String user : ss.list()) {
            ss.delete(user);
        }
        assertThat(ss.list().isEmpty(), is(true));
    }

    @Test
    public void testValidation()    {
        JsonObject valid = new JsonObject().put("version", 1);
        JsonObject invalid1 = new JsonObject();
        JsonObject invalid2 = new JsonObject().put("version", 2);

        ss.validateJsonVersion(valid);

        try {
            ss.validateJsonVersion(invalid1);
            fail("Invalid Json 1 didn't raised exception");
        } catch (RuntimeException e)    {
            // noop
        }

        try {
            ss.validateJsonVersion(invalid2);
            fail("Invalid Json 2 didn't raised exception");
        } catch (RuntimeException e)    {
            // noop
        }
    }

    @Test
    public void testDeletion()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        JsonObject updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));
    }

    @Test
    public void testUpdate()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        JsonObject updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(notNullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));
    }
}

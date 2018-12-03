/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.json.JsonObject;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ScramShaCredentialsTest {

    private static EmbeddedZooKeeper zkServer;

    private ScramShaCredentials ss;

    @BeforeClass
    public static void startZk() throws IOException, InterruptedException {
        zkServer = new EmbeddedZooKeeper();
    }

    @AfterClass
    public static void stopZk() {
        zkServer.close();
    }

    @Before
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
        assertTrue(ss.exists("userExists"));

    }

    @Test
    public void userNotExists() {
        assertFalse(ss.exists("userNotExists"));
    }

    @Test
    public void listSome() {
        ss.createOrUpdate("listSome", "foo-password");
        assertTrue(ss.list().contains("listSome"));
    }

    @Test
    public void listNone() {
        for (String user : ss.list()) {
            ss.delete(user);
        }
        assertTrue(ss.list().isEmpty());
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
        assertNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));
        assertEquals("somecredentials", updated.getJsonObject("config").getString("SCRAM-SHA-256"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.deleteUserJson(original.encode().getBytes(Charset.defaultCharset())), Charset.defaultCharset()));
        assertNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));
        assertEquals("somecredentials", updated.getJsonObject("config").getString("SCRAM-SHA-256"));
    }

    @Test
    public void testUpdate()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        JsonObject updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertNotNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertNotNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));
        assertEquals("somecredentials", updated.getJsonObject("config").getString("SCRAM-SHA-256"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertNotNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = new JsonObject(new String(ss.updateUserJson(original.encode().getBytes(Charset.defaultCharset()), "password"), Charset.defaultCharset()));
        assertNotNull(updated.getJsonObject("config").getString("SCRAM-SHA-512"));
        assertEquals("somecredentials", updated.getJsonObject("config").getString("SCRAM-SHA-256"));
    }
}

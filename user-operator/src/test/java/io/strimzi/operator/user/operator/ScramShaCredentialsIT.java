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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class ScramShaCredentialsIT {

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
    public void testUserExistsAfterCreate() {
        assertThat(scramShaCred.exists("userExists"), is(false));
        scramShaCred.createOrUpdate("userExists", "foo-password");
        assertThat(scramShaCred.exists("userExists"), is(true));
    }

    @Test
    public void testUserDoeNotExistPriorToCreate() {
        assertThat(scramShaCred.exists("userNotExists"), is(false));
    }

    @Test
    public void testCreateOrUpdate() {
        scramShaCred.createOrUpdate("normalCreate", "foo-password");
        assertThat(scramShaCred.exists("normalCreate"), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/normalCreate"), is(true));
    }

    @Test
    public void testCreateOrUpdateTwice() {
        scramShaCred.createOrUpdate("doubleCreate", "foo-password");
        scramShaCred.createOrUpdate("doubleCreate", "foo-password");
        assertThat(scramShaCred.exists("doubleCreate"), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/doubleCreate"), is(true));
    }

    @Test
    public void testDelete() {
        scramShaCred.createOrUpdate("normalDelete", "foo-password");
        assertThat(scramShaCred.exists("normalDelete"), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/normalDelete"), is(true));
        scramShaCred.delete("normalDelete");
        assertThat(scramShaCred.exists("normalDelete"), is(false));
        assertThat(scramShaCred.isPathExist("/config/users/normalDelete"), is(false));
    }

    @Test
    public void testDeleteTwice() {
        scramShaCred.createOrUpdate("doubleDelete", "foo-password");
        assertThat(scramShaCred.exists("doubleDelete"), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/doubleDelete"), is(true));

        scramShaCred.delete("doubleDelete");
        scramShaCred.delete("doubleDelete");
        assertThat(scramShaCred.exists("doubleDelete"), is(false));
        assertThat(scramShaCred.isPathExist("/config/users/doubleDelete"), is(false));
    }

    @Test
    public void testCreateOrUpdatePasswordUpdate() {
        scramShaCred.createOrUpdate("changePassword", "changePassword-password");
        scramShaCred.createOrUpdate("changePassword", "changePassword-password2");
        assertThat(scramShaCred.exists("changePassword"), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/changePassword"), is(true));
    }

    @Test
    public void testListListsCreatedUsers() {
        scramShaCred.createOrUpdate("listSome", "foo-password");
        assertThat(scramShaCred.list(), hasItem("listSome"));
    }

    @Test
    public void testListWithNoUsersReturnsEmptyList() {
        // Ensure all users deleted from other tests
        for (String user : scramShaCred.list()) {
            scramShaCred.delete(user);
        }
        assertThat(scramShaCred.list(), is(empty()));
    }

    @Test
    public void testValidation()    {
        JsonObject valid = new JsonObject().put("version", 1);
        JsonObject invalidEmptyJsonObject = new JsonObject();
        JsonObject invalidVersion = new JsonObject().put("version", 2);

        scramShaCred.validateJsonVersion(valid);

        assertThrows(RuntimeException.class, () -> scramShaCred.validateJsonVersion(invalidEmptyJsonObject),
                "Empty JsonObject should cause validate to throw Exception");

        assertThrows(RuntimeException.class, () -> scramShaCred.validateJsonVersion(invalidVersion),
                "Invalid version (!=1) should cause validate to throw Exception");
    }

    @Test
    public void testRemoveScramCredentialsFromUserJsonUser()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        JsonObject updated = scramShaCred.removeScramCredentialsFromUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials").put("SCRAM-SHA-256", "somecredentials"));
        updated = scramShaCred.removeScramCredentialsFromUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));

        original = new JsonObject().put("version", 1).put("config", new JsonObject());
        updated = scramShaCred.removeScramCredentialsFromUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));

        original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-256", "somecredentials"));
        updated = scramShaCred.removeScramCredentialsFromUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-256"), is("somecredentials"));
    }

    @Test
    public void testRemoveScramCredentialsFromJsonUserPersistsNonScramCredentialKeys()  {
        JsonObject original = new JsonObject().put("version", 1).put("config", new JsonObject().put("SCRAM-SHA-512", "somecredentials"));
        String keyThatShouldPersist = "persist";
        int valueThatShouldPersist = 42;
        original.getJsonObject("config").put(keyThatShouldPersist, valueThatShouldPersist);
        JsonObject updated = scramShaCred.removeScramCredentialsFromUserJson(original.encode().getBytes(StandardCharsets.UTF_8));
        assertThat(updated.getJsonObject("config").getString("SCRAM-SHA-512"), is(nullValue()));
        assertThat(updated.getJsonObject("config").getInteger(keyThatShouldPersist), is(valueThatShouldPersist));
    }

    @Test
    public void testUpdateUserJson()  {
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

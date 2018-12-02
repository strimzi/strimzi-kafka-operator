/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.test.EmbeddedZooKeeper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
}

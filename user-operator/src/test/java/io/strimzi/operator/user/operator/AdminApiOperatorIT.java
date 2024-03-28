/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.test.container.StrimziKafkaContainer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

@Timeout(10)
public abstract class AdminApiOperatorIT<T, S extends Collection<String>> {
    protected static final Logger LOGGER = LogManager.getLogger(AdminApiOperatorIT.class);

    public static final String SCRAM_USERNAME = "my-user";
    public static final String TLS_USERNAME = "CN=my-user";

    private static StrimziKafkaContainer kafkaContainer;
    protected static Admin adminClient;

    @BeforeAll
    public static void beforeAll() {
        Map<String, String> additionalConfiguration = Map.of(
                "authorizer.class.name", "kafka.security.authorizer.AclAuthorizer",
                "super.users", "User:ANONYMOUS");
        kafkaContainer = new StrimziKafkaContainer()
                .withBrokerId(1)
                .withKafkaConfigurationMap(additionalConfiguration);
        kafkaContainer.start();

        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        adminClient = Admin.create(p);
    }

    @AfterAll
    public static void afterAll() {
        if (adminClient != null) {
            adminClient.close();
        }

        kafkaContainer.stop();
    }

    abstract AdminApiOperator<T, S> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract T get(String username);
    abstract void assertResources(T expected, T actual);

    /**
     * This tests takes a SCRAM-SHA user and goes through a chain of operations and asserts each of them
     * - Lists all users (empty list is expected)
     * - Creates a user
     * - Lists all users (the user should be listed)
     * - Modifies the user
     * - Deletes the user
     * - Lists all users (should be empty)
     * - Deletes the user again
     */
    @Test
    public void testCreateModifyDeleteScramUsers() throws ExecutionException, InterruptedException {
        testCreateModifyDelete(SCRAM_USERNAME);
    }

    /**
     * This tests takes a LTS user and goes through a chain of operations and asserts each of them
     * - Lists all users (empty list is expected)
     * - Creates a user
     * - Lists all users (the user should be listed)
     * - Modifies the user
     * - Deletes the user
     * - Lists all users (should be empty)
     * - Deletes the user again
     */
    @Test
    public void testCreateModifyDeleteTlsUsers() throws ExecutionException, InterruptedException {
        testCreateModifyDelete(TLS_USERNAME);
    }

    /**
     * This tests goes through a chain of operations and asserts each of them
     * - Lists all users (empty list is expected)
     * - Creates a user
     * - Lists all users (the user should be listed)
     * - Modifies the user
     * - Deletes the user
     * - Lists all users (should be empty)
     * - Deletes the user again
     *
     * @param username   Username
     */
    public void testCreateModifyDelete(String username) throws ExecutionException, InterruptedException {
        AdminApiOperator<T, S> op = operator();
        op.start();

        try {
            T newResource = getOriginal();
            T modResource = getModified();

            // Get all users => should be empty
            LOGGER.info("Checking empty list without users");
            CompletionStage<S> allUsers = op.getAllUsers();
            S userList = allUsers.toCompletableFuture().get();
            LOGGER.info("Found users. {}", userList);
            assertThat(userList.isEmpty(), is(true));

            // Create new user => should be created
            LOGGER.info("Checking user creation");
            CompletionStage<ReconcileResult<T>> reconcile = op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, newResource);
            ReconcileResult<T> reconcileResult = reconcile.toCompletableFuture().get();
            assertThat(reconcileResult.getType(), is(createPatches() ? ReconcileResult.Type.PATCHED : ReconcileResult.Type.CREATED));
            T created = get(username);
            assertThat(created, is(notNullValue()));
            assertResources(newResource, created);

            // Get all users => should have the new user listed now
            LOGGER.info("Checking user list => now with a users");
            allUsers = op.getAllUsers();
            userList = allUsers.toCompletableFuture().get();
            LOGGER.info("Found users. {}", userList);
            assertThat(userList, contains(KafkaUserModel.decodeUsername(username)));

            // Modify user => should be modified
            LOGGER.info("Checking user modification");
            reconcile = op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, modResource);
            reconcileResult = reconcile.toCompletableFuture().get();
            assertThat(reconcileResult.getType(), is(ReconcileResult.Type.PATCHED));
            T modified = get(username);
            assertThat(modified, is(notNullValue()));
            assertResources(modResource, modified);

            // Delete user => should be deleted
            LOGGER.info("Checking user deletion");
            reconcile = op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, null);
            reconcileResult = reconcile.toCompletableFuture().get();
            assertThat(reconcileResult.getType(), is(ReconcileResult.Type.DELETED));
            T deleted = get(username);
            assertThat(deleted, is(nullValue()));

            // Get all users => should be empty again
            LOGGER.info("Checking empty list after deletion");
            allUsers = op.getAllUsers();
            userList = allUsers.toCompletableFuture().get();
            LOGGER.info("Found users. {}", userList);
            assertThat(userList.isEmpty(), is(true));

            // Deleting deleted user user => should be noop
            LOGGER.info("Checking deletion of non-existent user");
            reconcile = op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, null);
            reconcileResult = reconcile.toCompletableFuture().get();
            assertThat(reconcileResult.getType(), is(ReconcileResult.Type.NOOP));
            deleted = get(username);
            assertThat(deleted, is(nullValue()));
        } finally {
            op.stop();
        }
    }

    // In some cases, the creation returns PATCH as reconcile result instead of CREATED
    // This method can be used to override the default behavior in tests where this is the case (e.g. with SCRAM-SHA credentials)
    protected boolean createPatches()   {
        return false;
    }
}
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.test.container.StrimziKafkaContainer;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

@ExtendWith(VertxExtension.class)
public abstract class AbstractAdminApiOperatorIT<T, S extends Collection<String>> {
    protected static final Logger LOGGER = LogManager.getLogger(AbstractAdminApiOperatorIT.class);

    public static final String SCRAM_USERNAME = "my-user";
    public static final String TLS_USERNAME = "CN=my-user";

    private static StrimziKafkaContainer kafkaContainer;
    protected static Vertx vertx;
    protected static Admin adminClient;

    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
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
        if (vertx != null) {
            vertx.close();
        }

        if (adminClient != null) {
            adminClient.close();
        }

        kafkaContainer.stop();
    }

    abstract AbstractAdminApiOperator<T, S> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract T get(String username);
    abstract void assertResources(VertxTestContext context, T expected, T actual);

    /**
     * This tests takes a SCRAM-SHA user and goes through a chain of operations and asserts each of them
     * - Lists all users (empty list is expected)
     * - Creates a user
     * - Lists all users (the user should be listed)
     * - Modifies the user
     * - Deletes the user
     * - Lists all users (should be empty)
     * - Deletes the user again
     *
     * @param context   Test context
     */
    @Test
    public void testCreateModifyDeleteScramUsers(VertxTestContext context)    {
        testCreateModifyDelete(context, SCRAM_USERNAME);
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
     *
     * @param context   Test context
     */
    @Test
    public void testCreateModifyDeleteTlsUsers(VertxTestContext context)    {
        testCreateModifyDelete(context, TLS_USERNAME);
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
     * @param context   Test context
     */
    public void testCreateModifyDelete(VertxTestContext context, String username)    {
        Checkpoint async = context.checkpoint();

        AbstractAdminApiOperator<T, S> op = operator();

        T newResource = getOriginal();
        T modResource = getModified();

        op.getAllUsers()
                .onComplete(context.succeeding(noUsers -> {
                    LOGGER.info("Asserting existing");
                    context.verify(() -> assertThat(noUsers.isEmpty(), is(true)));
                }))
                .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, newResource))
                .onComplete(context.succeeding(rrCreated -> {
                    LOGGER.info("Asserting created");
                    T created = get(username);
                    LOGGER.info("Asserting created 2");
                    context.verify(() -> assertThat(created, Matchers.is(Matchers.notNullValue())));
                    assertResources(context, newResource, created);
                }))
                .compose(rr -> op.getAllUsers())
                .onComplete(context.succeeding(userList -> {
                    LOGGER.info("Asserting existing");
                    context.verify(() -> assertThat(userList, contains(KafkaUserModel.decodeUsername(username))));
                }))
                .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, modResource))
                .onComplete(context.succeeding(rrModified -> {
                    LOGGER.info("Asserting modified");
                    T modified = get(username);

                    context.verify(() -> assertThat(modified, Matchers.is(Matchers.notNullValue())));
                    assertResources(context, modResource, modified);
                }))
                .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, null))
                .onComplete(context.succeeding(rrDeleted -> {
                    LOGGER.info("Asserting deleted");
                    T modified = get(username);
                    context.verify(() -> assertThat(modified, Matchers.is(Matchers.nullValue())));
                }))
                .compose(rr -> op.getAllUsers())
                .onComplete(context.succeeding(noUsers -> {
                    LOGGER.info("Asserting existing");
                    context.verify(() -> {
                        assertThat(noUsers.isEmpty(), is(true));
                    });
                }))
                .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, null))
                .onComplete(context.succeeding(rrDeleted -> {
                    LOGGER.info("Asserting deleted credentials");
                    T modified = get(username);
                    context.verify(() -> assertThat(modified, Matchers.is(Matchers.nullValue())));
                    async.flag();
                }));
    }
}

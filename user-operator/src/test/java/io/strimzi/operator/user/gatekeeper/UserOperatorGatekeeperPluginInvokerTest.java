/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.gatekeeper;

import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.api.kafka.model.user.KafkaUserStatusBuilder;
import io.strimzi.operator.common.gatekeeper.GatekeeperPluginFactory;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserValidatingPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UserOperatorGatekeeperPluginInvokerTest {
    private final List<String> order = new ArrayList<>();

    @AfterEach
    public void cleanup() {
        GatekeeperPluginFactory.clearTestPlugins();
    }

    @Test
    public void testEntryChainsMutatingPluginsInRequestedOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new UserMutator("A"), new UserMutator("B")));

        KafkaUser result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserEntry(null, user(""))
                .toCompletableFuture().join();

        assertThat(result.getMetadata().getName(), is("AB"));
        assertThat(order, contains("A", "B"));
    }

    @Test
    public void testExitChainsPluginsInReverseOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new UserMutator("A"), new UserMutator("B")));

        UserOperatorGatekeeperPluginInvoker
                .kafkaUserExit(null, user(""), new KafkaUserStatus())
                .toCompletableFuture().join();

        assertThat(order, contains("B", "A"));
    }

    @Test
    public void testMutatingAndValidatingPluginsRunInOneOrderedChain() {
        GatekeeperPluginFactory.initializeForTests(List.of(new UserMutator("A"), new UserValidator("B", false)));

        KafkaUser result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserEntry(null, user(""))
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
        assertThat(result.getMetadata().getName(), is("A"));
    }

    @Test
    public void testEntryWithNoPluginsReturnsResourceUnchanged() {
        GatekeeperPluginFactory.initializeForTests(List.of());

        KafkaUser result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserEntry(null, user("unchanged"))
                .toCompletableFuture().join();

        assertThat(result.getMetadata().getName(), is("unchanged"));
        assertThat(order, is(empty()));
    }

    @Test
    public void testChainStopsAtFirstValidationFailure() {
        GatekeeperPluginFactory.initializeForTests(List.of(new UserValidator("fail", true), new UserValidator("ok", false)));

        CompletableFuture<KafkaUser> result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserEntry(null, user(""))
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
        assertThat(order, contains("fail"));
    }

    @Test
    public void testValidatingPluginCannotModifyTheResource() {
        KafkaUser base = user("original");
        GatekeeperPluginFactory.initializeForTests(List.of(new UserResourceMutatingValidator()));

        KafkaUser result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserEntry(null, base)
                .toCompletableFuture().join();

        assertThat(result.getMetadata().getName(), is("original"));
        assertThat(base.getMetadata().getName(), is("original"));
    }

    @Test
    public void testValidatingPluginCopyIsNullTolerant() {
        CapturingUserValidator validator = new CapturingUserValidator();
        GatekeeperPluginFactory.initializeForTests(List.of(validator));

        KafkaUser result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserEntry(null, null)
                .toCompletableFuture().join();

        assertThat(result, is(nullValue()));
        assertThat(validator.invoked, is(true));
        assertThat(validator.receivedUser, is(nullValue()));
    }

    @Test
    public void testExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new StatusMutator()));

        KafkaUserStatus status = new KafkaUserStatusBuilder()
                .withObservedGeneration(1L)
                .build();

        KafkaUserStatus result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserExit(null, user("my-user"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testExitValidatingPluginCannotModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new StatusMutatingValidator()));

        KafkaUserStatus status = new KafkaUserStatusBuilder()
                .withObservedGeneration(1L)
                .build();

        KafkaUserStatus result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserExit(null, user("my-user"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1L));
    }

    @Test
    public void testDeletionInvokesAllPluginsInRequestedOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new UserDeletionPlugin("A", false), new UserDeletionPlugin("B", false)));

        UserOperatorGatekeeperPluginInvoker
                .kafkaUserDeletion(null, "my-namespace", "my-user")
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
    }

    @Test
    public void testDeletionPassesNamespaceAndName() {
        UserDeletionPlugin plugin = new UserDeletionPlugin("A", false);
        GatekeeperPluginFactory.initializeForTests(List.of(plugin));

        UserOperatorGatekeeperPluginInvoker
                .kafkaUserDeletion(null, "my-namespace", "my-user")
                .toCompletableFuture().join();

        assertThat(plugin.namespace, is("my-namespace"));
        assertThat(plugin.name, is("my-user"));
    }

    @Test
    public void testDeletionStopsAtFirstFailure() {
        GatekeeperPluginFactory.initializeForTests(List.of(new UserDeletionPlugin("fail", true), new UserDeletionPlugin("ok", false)));

        CompletableFuture<Void> result = UserOperatorGatekeeperPluginInvoker
                .kafkaUserDeletion(null, "my-namespace", "my-user")
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
        assertThat(order, contains("fail"));
    }

    @Test
    public void testDeletionWithValidatingPlugin() {
        UserValidatingDeletionPlugin plugin = new UserValidatingDeletionPlugin("A", false);
        GatekeeperPluginFactory.initializeForTests(List.of(plugin));

        UserOperatorGatekeeperPluginInvoker
                .kafkaUserDeletion(null, "my-namespace", "my-user")
                .toCompletableFuture().join();

        assertThat(order, contains("A"));
        assertThat(plugin.namespace, is("my-namespace"));
        assertThat(plugin.name, is("my-user"));
    }

    private static KafkaUser user(String name) {
        return new KafkaUserBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static String append(String current, String tag) {
        return (current == null ? "" : current) + tag;
    }

    private final class UserMutator implements GatekeeperKafkaUserMutatingPlugin {
        private final String tag;

        private UserMutator(String tag) {
            this.tag = tag;
        }

        @Override
        public CompletionStage<KafkaUser> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            order.add(tag);
            return CompletableFuture.completedFuture(new KafkaUserBuilder(kafkaUser)
                    .editOrNewMetadata()
                        .withName(append(kafkaUser.getMetadata().getName(), tag))
                    .endMetadata()
                    .build());
        }

        @Override
        public CompletionStage<KafkaUserStatus> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
            order.add(tag);
            return CompletableFuture.completedFuture(newKafkaUserStatus);
        }
    }

    private final class UserValidator implements GatekeeperKafkaUserValidatingPlugin {
        private final String tag;
        private final boolean fail;

        private UserValidator(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }
    }

    private static final class UserResourceMutatingValidator implements GatekeeperKafkaUserValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            kafkaUser.getMetadata().setName("HACKED");
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class CapturingUserValidator implements GatekeeperKafkaUserValidatingPlugin {
        private boolean invoked;
        private KafkaUser receivedUser;

        @Override
        public CompletionStage<Void> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            invoked = true;
            receivedUser = kafkaUser;
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class StatusMutator implements GatekeeperKafkaUserMutatingPlugin {
        @Override
        public CompletionStage<KafkaUserStatus> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
            newKafkaUserStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaUserStatus);
        }
    }

    private static final class StatusMutatingValidator implements GatekeeperKafkaUserValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
            newKafkaUserStatus.setObservedGeneration(1919L);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class UserDeletionPlugin implements GatekeeperKafkaUserMutatingPlugin {
        private final String tag;
        private final boolean fail;
        private String namespace;
        private String name;

        private UserDeletionPlugin(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
            order.add(tag);
            this.namespace = namespace;
            this.name = name;
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }
    }

    private final class UserValidatingDeletionPlugin implements GatekeeperKafkaUserValidatingPlugin {
        private final String tag;
        private final boolean fail;
        private String namespace;
        private String name;

        private UserValidatingDeletionPlugin(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
            order.add(tag);
            this.namespace = namespace;
            this.name = name;
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }
    }
}
